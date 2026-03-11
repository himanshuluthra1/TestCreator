"""Excel Comparison Web Application.

Upload two Excel files, configure column-level comparison criteria
(column mapping, datatype, operator), then download:
  - matched.xlsx   – rows from file 2 that matched at least one row in file 1
  - unmatched.xlsx – rows from file 1 that did not match any row in file 2
"""

import io
import json
import os
import re
import shutil
import uuid
import zipfile
import calendar
from datetime import datetime, timezone

import pandas as pd
from flask import (
    Flask,
    jsonify,
    redirect,
    render_template,
    request,
    send_file,
    session,
    url_for,
)

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", os.urandom(24))

# Directories for temporary uploads and generated outputs
UPLOAD_FOLDER = os.path.join(os.path.dirname(__file__), "uploads")
OUTPUT_FOLDER = os.path.join(os.path.dirname(__file__), "outputs")

os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

ALLOWED_EXTENSIONS = {"xlsx", "xls"}

# Supported operators per datatype category
OPERATORS = {
    "string": [
        ("eq", "Equal (=)"),
        ("ne", "Not Equal (≠)"),
        ("contains", "Contains"),
        ("startswith", "Starts With"),
        ("endswith", "Ends With"),
    ],
    "number": [
        ("eq", "Equal (=)"),
        ("ne", "Not Equal (≠)"),
        ("gt", "Greater Than (>)"),
        ("gte", "Greater Than or Equal (≥)"),
        ("lt", "Less Than (<)"),
        ("lte", "Less Than or Equal (≤)"),
    ],
    "date": [
        ("eq", "Equal (=)"),
        ("ne", "Not Equal (≠)"),
        ("gt", "After (>)"),
        ("gte", "On or After (≥)"),
        ("lt", "Before (<)"),
        ("lte", "On or Before (≤)"),
    ],
    "boolean": [
        ("eq", "Equal (=)"),
        ("ne", "Not Equal (≠)"),
    ],
}

DATATYPES = [
    ("string", "Text / String"),
    ("number", "Number (Integer / Decimal)"),
    ("date", "Date / DateTime"),
    ("boolean", "Boolean (True / False)"),
]

DEFAULT_SAMPLE_FILE1 = "Order.xlsx"
DEFAULT_SAMPLE_FILE2 = "Saanch - Copy.xlsx"
DEFAULT_SAMPLE_COMPARISON_FILES = [
    "Saanch - Copy.xlsx",
    "GPVDS - Copy.xlsx",
    "Vertex - Copy.xlsx",
]
MAX_UPLOAD_FILES = 8
EXPIRY_COLUMN_NAME = "Expiry Date"
EXPIRY_WINDOW_OPTIONS = [
    (3, "3 Months"),
    (6, "6 Months"),
    (9, "9 Months"),
    (12, "12 Months"),
]
DEFAULT_EXPIRY_WINDOW_MONTHS = 9


def allowed_file(filename: str) -> bool:
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXTENSIONS


def safe_filename(filename: str) -> str:
    """Return a safe version of the filename with a unique prefix."""
    ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else "xlsx"
    return f"{uuid.uuid4().hex}.{ext}"


def read_excel(path: str) -> pd.DataFrame:
    """Read an Excel file and return a DataFrame with all values as strings
    (raw) in addition to a parsed version."""
    raw_df = pd.read_excel(path, header=None, dtype=str, keep_default_na=False)

    # Some stock files include banner/meta rows before the real header.
    # If first-column value "Drug Code" exists near the top, treat that row as header.
    if not raw_df.empty and raw_df.shape[1] > 0:
        max_scan_rows = min(len(raw_df), 50)
        for row_index in range(max_scan_rows):
            first_col_value = str(raw_df.iat[row_index, 0]).strip().lower()
            if first_col_value == "drug code":
                headers = [str(value).strip() for value in raw_df.iloc[row_index].tolist()]
                parsed_df = raw_df.iloc[row_index + 1 :].copy()
                parsed_df.columns = headers
                non_blank_columns = [
                    col for col in parsed_df.columns if col and str(col).strip().lower() != "nan"
                ]
                parsed_df = parsed_df.loc[:, non_blank_columns]
                return parsed_df.fillna("")

    return pd.read_excel(path, dtype=str, keep_default_na=False)


def cast_series(series: pd.Series, datatype: str) -> pd.Series:
    """Cast a pandas Series to the requested datatype."""
    if datatype == "number":
        return pd.to_numeric(series, errors="coerce")
    if datatype == "date":
        return pd.to_datetime(series, errors="coerce")
    if datatype == "boolean":
        return series.str.strip().str.lower().map(
            {"true": True, "1": True, "yes": True,
             "false": False, "0": False, "no": False}
        )
    # string – strip whitespace for cleaner comparison
    return series.str.strip()


def apply_operator(left: pd.Series, right_scalar, operator: str) -> pd.Series:
    """Return a boolean Series applying *operator* between left Series and
    a scalar value (right_scalar)."""
    if operator == "eq":
        return left == right_scalar
    if operator == "ne":
        return left != right_scalar
    if operator == "gt":
        return left > right_scalar
    if operator == "gte":
        return left >= right_scalar
    if operator == "lt":
        return left < right_scalar
    if operator == "lte":
        return left <= right_scalar
    if operator == "contains":
        needle = str(right_scalar).strip()
        if not needle:
            return pd.Series([False] * len(left), index=left.index)
        # Match as a phrase boundary to avoid accidental matches inside words
        # (e.g., "pen" should not match "open box").
        pattern = rf"(?<![0-9A-Za-z]){re.escape(needle)}(?![0-9A-Za-z])"
        return left.fillna("").astype(str).str.contains(pattern, na=False, case=False, regex=True)
    if operator == "startswith":
        needle = str(right_scalar).strip()
        if not needle:
            return pd.Series([False] * len(left), index=left.index)
        return left.fillna("").astype(str).str.startswith(needle)
    if operator == "endswith":
        needle = str(right_scalar).strip()
        if not needle:
            return pd.Series([False] * len(left), index=left.index)
        return left.fillna("").astype(str).str.endswith(needle)
    raise ValueError(f"Unknown operator: {operator}")


def apply_scalar_operator(left_scalar, right_scalar, operator: str) -> bool:
    """Apply numeric comparison operators to scalar values."""
    if operator == "eq":
        return left_scalar == right_scalar
    if operator == "ne":
        return left_scalar != right_scalar
    if operator == "gt":
        return left_scalar > right_scalar
    if operator == "gte":
        return left_scalar >= right_scalar
    if operator == "lt":
        return left_scalar < right_scalar
    if operator == "lte":
        return left_scalar <= right_scalar
    raise ValueError(f"Unsupported scalar operator: {operator}")


def reverse_order_operator(operator: str) -> str:
    """Return equivalent operator when operands are swapped.

    Example: a > b  <=>  b < a
    """
    mapping = {
        "gt": "lt",
        "gte": "lte",
        "lt": "gt",
        "lte": "gte",
    }
    return mapping.get(operator, operator)


def add_months(base_date: datetime, months: int) -> datetime:
    """Return base_date shifted by *months* while keeping day-of-month when possible."""
    month_index = base_date.month - 1 + months
    year = base_date.year + month_index // 12
    month = month_index % 12 + 1
    last_day = calendar.monthrange(year, month)[1]
    day = min(base_date.day, last_day)
    return base_date.replace(year=year, month=month, day=day)


def filter_by_expiry_window(df: pd.DataFrame, months: int) -> pd.DataFrame:
    """Keep rows whose Expiry Date is beyond today + months.

    Supported formats:
    - MM-DD-YYYY (preferred, e.g. 03-31-2026)
    - MMM-YYYY (legacy, e.g. Mar-2026)
    - YYYY-MM-DD or full datetime text (e.g. 2026-03-31 00:00:00)
    - Excel serial date numbers
    """
    if EXPIRY_COLUMN_NAME not in df.columns:
        raise ValueError(f"Missing required comparison column: {EXPIRY_COLUMN_NAME}")

    cutoff_date = add_months(datetime.now(), months).date()

    raw_expiry_values = df[EXPIRY_COLUMN_NAME].astype(str).str.strip()

    # Parse explicit known formats first, then fall back to flexible parsing.
    parsed_mmddyyyy = pd.to_datetime(raw_expiry_values, format="%m-%d-%Y", errors="coerce")
    parsed_mmmyyyy = pd.to_datetime(raw_expiry_values, format="%b-%Y", errors="coerce")
    parsed_general = pd.to_datetime(raw_expiry_values, errors="coerce", format="mixed")
    parsed_general_dayfirst = pd.to_datetime(
        raw_expiry_values,
        errors="coerce",
        dayfirst=True,
        format="mixed",
    )

    # Some Excel exports may contain serial day numbers as strings.
    numeric_serial = pd.to_numeric(raw_expiry_values, errors="coerce")
    parsed_excel_serial = pd.to_datetime(numeric_serial, unit="D", origin="1899-12-30", errors="coerce")

    expiry_dates = (
        parsed_mmddyyyy
        .fillna(parsed_mmmyyyy)
        .fillna(parsed_general)
        .fillna(parsed_general_dayfirst)
        .fillna(parsed_excel_serial)
        .dt.date
    )

    valid_mask = expiry_dates.notna() & (expiry_dates > cutoff_date)
    return df.loc[valid_mask].copy()


def compare_dataframes(
    df1: pd.DataFrame,
    df2: pd.DataFrame,
    criteria: list[dict],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Compare df1 and df2 using the provided criteria list.

    Each criterion is a dict with keys:
        col1     – column name in df1
        col2     – column name in df2
        datatype – one of string / number / date / boolean
        operator – comparison operator string

    Returns:
        matched   – rows from df2 whose values matched at least one row in df1
                    for ALL criteria simultaneously.
        unmatched – rows from df1 that did NOT match any row in df2.
    """
    if not criteria:
        return pd.DataFrame(columns=df2.columns), df1.copy()

    # Build cast versions of the relevant columns
    df1_cast = df1.copy()
    df2_cast = df2.copy()

    for crit in criteria:
        col1, col2, datatype = crit["col1"], crit["col2"], crit["datatype"]
        df1_cast[col1] = cast_series(df1_cast[col1], datatype)
        df2_cast[col2] = cast_series(df2_cast[col2], datatype)

    matched_df2_indices = set()
    matched_df1_indices = set()

    non_number_criteria = [c for c in criteria if c["datatype"] != "number"]
    number_criteria = [c for c in criteria if c["datatype"] == "number"]

    for i1, row1 in df1_cast.iterrows():
        # If number criteria are present with other criteria, sum matching number
        # values in df2 and compare total against df1 numeric value.
        if number_criteria and non_number_criteria:
            base_mask = pd.Series([True] * len(df2_cast), index=df2_cast.index)
            for crit in non_number_criteria:
                col1, col2, operator = crit["col1"], crit["col2"], crit["operator"]
                row_mask = apply_operator(df2_cast[col2], row1[col1], operator)
                base_mask = base_mask & row_mask

            if not base_mask.any():
                continue

            numeric_match = True
            for crit in number_criteria:
                col1, col2, operator = crit["col1"], crit["col2"], crit["operator"]
                total_value = pd.to_numeric(
                    df2_cast.loc[base_mask, col2], errors="coerce"
                ).sum(min_count=1)
                target_value = row1[col1]

                if pd.isna(total_value) or pd.isna(target_value):
                    numeric_match = False
                    break

                if not apply_scalar_operator(target_value, total_value, operator):
                    numeric_match = False
                    break

            if numeric_match:
                matched_df1_indices.add(i1)
                matched_df2_indices.update(df2_cast.index[base_mask].tolist())
            continue

        # Default behavior: row-wise criteria match.
        mask = pd.Series([True] * len(df2_cast), index=df2_cast.index)
        for crit in criteria:
            col1, col2 = crit["col1"], crit["col2"]
            operator = crit["operator"]
            datatype = crit["datatype"]

            # apply_operator evaluates series OP scalar as (df2 OP df1).
            # For numeric comparisons, users expect (df1 OP df2), so reverse.
            if datatype == "number":
                operator = reverse_order_operator(operator)

            row_mask = apply_operator(df2_cast[col2], row1[col1], operator)
            mask = mask & row_mask

        if mask.any():
            matched_df1_indices.add(i1)
            matched_df2_indices.update(df2_cast.index[mask].tolist())

    unmatched_df1_indices = [i for i in df1.index if i not in matched_df1_indices]

    matched = df2.loc[sorted(matched_df2_indices)].reset_index(drop=True)
    unmatched = df1.loc[unmatched_df1_indices].copy()

    return matched, unmatched


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------


def load_default_samples_into_session() -> bool:
    """Load bundled sample files into session context.

    Returns True when all required sample files are available and readable.
    """
    sample_dir = os.path.join(os.path.dirname(__file__), "sample_data")
    source1 = os.path.join(sample_dir, DEFAULT_SAMPLE_FILE1)
    comparison_sources = [
        os.path.join(sample_dir, sample_name)
        for sample_name in DEFAULT_SAMPLE_COMPARISON_FILES
    ]

    if not os.path.exists(source1):
        return False
    if any(not os.path.exists(path) for path in comparison_sources):
        return False

    # Copy into uploads so downstream compare flow can use the same code path.
    dest1_name = "default_sample_file1.xlsx"
    dest1 = os.path.join(UPLOAD_FOLDER, dest1_name)
    shutil.copyfile(source1, dest1)

    copied_comparisons = []
    for idx, sample_name in enumerate(DEFAULT_SAMPLE_COMPARISON_FILES, start=2):
        source_path = os.path.join(sample_dir, sample_name)
        dest_name = f"default_sample_file{idx}.xlsx"
        dest_path = os.path.join(UPLOAD_FOLDER, dest_name)
        shutil.copyfile(source_path, dest_path)
        copied_comparisons.append({"stored": dest_name, "original": sample_name})

    try:
        df1 = read_excel(dest1)
        df2 = read_excel(os.path.join(UPLOAD_FOLDER, copied_comparisons[0]["stored"]))
    except Exception:  # noqa: BLE001
        return False

    session["file1"] = dest1_name
    session["file2"] = copied_comparisons[0]["stored"]
    session["comparison_files"] = copied_comparisons
    session["original1"] = DEFAULT_SAMPLE_FILE1
    session["original2"] = copied_comparisons[0]["original"]
    session["cols1"] = list(df1.columns)
    session["cols2"] = list(df2.columns)
    return True


def copy_sample_file_to_uploads(sample_name: str, destination_name: str) -> str | None:
    """Copy a bundled sample file into uploads and return stored filename.

    Returns None when the source sample file does not exist.
    """
    sample_dir = os.path.join(os.path.dirname(__file__), "sample_data")
    source_path = os.path.join(sample_dir, sample_name)
    if not os.path.exists(source_path):
        return None

    dest_path = os.path.join(UPLOAD_FOLDER, destination_name)
    shutil.copyfile(source_path, dest_path)
    return destination_name


def get_comparison_file_entries() -> list[dict[str, str]]:
    """Return comparison file metadata from session.

    New format uses session["comparison_files"] as a list of
    {"stored": ..., "original": ...}. Older sessions with only file2/original2
    are still supported for backward compatibility.
    """
    entries = session.get("comparison_files") or []
    if entries:
        return entries

    legacy_file2 = session.get("file2")
    if not legacy_file2:
        return []

    return [
        {
            "stored": legacy_file2,
            "original": session.get("original2", "File 2"),
        }
    ]


@app.route("/", methods=["GET"])
def home():
    return render_template("home.html")


@app.route("/order-creator", methods=["GET"])
def order_creator():
    return render_template(
        "index.html",
        default_file1=f"sample_data/{DEFAULT_SAMPLE_FILE1}",
        default_file2=f"sample_data/{DEFAULT_SAMPLE_FILE2}",
        default_comparison_files=DEFAULT_SAMPLE_COMPARISON_FILES,
        max_upload_files=MAX_UPLOAD_FILES,
    )


@app.route("/stock-checker", methods=["GET"])
def stock_checker():
    return render_template("stock_checker.html")


@app.route("/upload", methods=["POST"])
def upload():
    """Accept an order file plus up to 7 comparison files.

    File 1 is the base order file. Files 2..8 are optional comparison files
    that will be used sequentially in /compare.
    """
    file1 = request.files.get("file1")
    file2 = request.files.get("file2")
    optional_comparison_uploads = [request.files.get(f"file{i}") for i in range(3, MAX_UPLOAD_FILES + 1)]
    all_uploads = [file1, file2, *optional_comparison_uploads]

    file1_missing = (not file1) or file1.filename == ""
    file2_missing = (not file2) or file2.filename == ""
    provided_optional_comparison_uploads = [f for f in optional_comparison_uploads if f and f.filename != ""]
    comparisons_missing = file2_missing and len(provided_optional_comparison_uploads) == 0
    use_sample_set = request.form.get("use_sample_set") == "1"
    has_any_uploaded_file = any(upload and upload.filename != "" for upload in all_uploads)

    if use_sample_set and not has_any_uploaded_file:
        if load_default_samples_into_session():
            return redirect(url_for("configure"))
        return render_template(
            "index.html",
            errors=[
                "Bundled sample files are not available. "
                "Please add Order.xlsx, Saanch - Copy.xlsx, GPVDS - Copy.xlsx, and Vertex - Copy.xlsx under sample_data/."
            ],
            default_file1=f"sample_data/{DEFAULT_SAMPLE_FILE1}",
            default_file2=f"sample_data/{DEFAULT_SAMPLE_FILE2}",
            default_comparison_files=DEFAULT_SAMPLE_COMPARISON_FILES,
            max_upload_files=MAX_UPLOAD_FILES,
        )

    # If user submits without selecting files, fall back to bundled defaults.
    if file1_missing and comparisons_missing:
        if load_default_samples_into_session():
            return redirect(url_for("configure"))
        return render_template(
            "index.html",
            errors=["Default sample files are not available."],
            default_file1=f"sample_data/{DEFAULT_SAMPLE_FILE1}",
            default_file2=f"sample_data/{DEFAULT_SAMPLE_FILE2}",
            default_comparison_files=DEFAULT_SAMPLE_COMPARISON_FILES,
            max_upload_files=MAX_UPLOAD_FILES,
        )

    errors = []
    if not file1_missing and not allowed_file(file1.filename):
        errors.append("File 1 must be an Excel file (.xlsx or .xls).")

    if not file2_missing and not allowed_file(file2.filename):
        errors.append("File 2 must be an Excel file (.xlsx or .xls).")

    for index, comp_file in enumerate(optional_comparison_uploads, start=3):
        if comp_file and comp_file.filename and not allowed_file(comp_file.filename):
            errors.append(f"File {index} must be an Excel file (.xlsx or .xls).")

    if errors:
        return render_template(
            "index.html",
            errors=errors,
            default_file1=f"sample_data/{DEFAULT_SAMPLE_FILE1}",
            default_file2=f"sample_data/{DEFAULT_SAMPLE_FILE2}",
            default_comparison_files=DEFAULT_SAMPLE_COMPARISON_FILES,
            max_upload_files=MAX_UPLOAD_FILES,
        )

    if file1_missing:
        fname1 = copy_sample_file_to_uploads(DEFAULT_SAMPLE_FILE1, "default_sample_file1.xlsx")
        if not fname1:
            return render_template(
                "index.html",
                errors=[f"Default sample file is missing: sample_data/{DEFAULT_SAMPLE_FILE1}"],
                default_file1=f"sample_data/{DEFAULT_SAMPLE_FILE1}",
                default_file2=f"sample_data/{DEFAULT_SAMPLE_FILE2}",
                default_comparison_files=DEFAULT_SAMPLE_COMPARISON_FILES,
                max_upload_files=MAX_UPLOAD_FILES,
            )
        path1 = os.path.join(UPLOAD_FOLDER, fname1)
        original_file1_name = DEFAULT_SAMPLE_FILE1
    else:
        fname1 = safe_filename(file1.filename)
        path1 = os.path.join(UPLOAD_FOLDER, fname1)
        file1.save(path1)
        original_file1_name = file1.filename

    saved_comparison_entries = []
    if file2_missing:
        default_file2_stored = copy_sample_file_to_uploads(DEFAULT_SAMPLE_FILE2, "default_sample_file2.xlsx")
        if not default_file2_stored:
            return render_template(
                "index.html",
                errors=[f"Default sample file is missing: sample_data/{DEFAULT_SAMPLE_FILE2}"],
                default_file1=f"sample_data/{DEFAULT_SAMPLE_FILE1}",
                default_file2=f"sample_data/{DEFAULT_SAMPLE_FILE2}",
                default_comparison_files=DEFAULT_SAMPLE_COMPARISON_FILES,
                max_upload_files=MAX_UPLOAD_FILES,
            )
        saved_comparison_entries.append({"stored": default_file2_stored, "original": DEFAULT_SAMPLE_FILE2})
    else:
        file2_name = safe_filename(file2.filename)
        file2_path = os.path.join(UPLOAD_FOLDER, file2_name)
        file2.save(file2_path)
        saved_comparison_entries.append({"stored": file2_name, "original": file2.filename})

    for comp_file in provided_optional_comparison_uploads:
        comp_name = safe_filename(comp_file.filename)
        comp_path = os.path.join(UPLOAD_FOLDER, comp_name)
        comp_file.save(comp_path)
        saved_comparison_entries.append({"stored": comp_name, "original": comp_file.filename})

    try:
        df1 = read_excel(path1)
        df2 = read_excel(os.path.join(UPLOAD_FOLDER, saved_comparison_entries[0]["stored"]))
    except Exception as exc:  # noqa: BLE001
        return render_template(
            "index.html",
            errors=[f"Could not read Excel file: {exc}"],
            default_file1=f"sample_data/{DEFAULT_SAMPLE_FILE1}",
            default_file2=f"sample_data/{DEFAULT_SAMPLE_FILE2}",
            default_comparison_files=DEFAULT_SAMPLE_COMPARISON_FILES,
            max_upload_files=MAX_UPLOAD_FILES,
        )

    session["file1"] = fname1
    session["file2"] = saved_comparison_entries[0]["stored"]
    session["comparison_files"] = saved_comparison_entries
    session["original1"] = original_file1_name
    session["original2"] = saved_comparison_entries[0]["original"]
    session["cols1"] = list(df1.columns)
    session["cols2"] = list(df2.columns)

    return redirect(url_for("configure"))


@app.route("/configure", methods=["GET"])
def configure():
    cols1 = session.get("cols1", [])
    cols2 = session.get("cols2", [])
    if not cols1 or not cols2:
        return redirect(url_for("order_creator"))

    return render_template(
        "configure.html",
        cols1=cols1,
        cols2=cols2,
        datatypes=DATATYPES,
        operators_json=json.dumps(OPERATORS),
        original1=session.get("original1", "File 1"),
        original2=session.get("original2", "File 2"),
        comparison_count=len(get_comparison_file_entries()),
        expiry_options=EXPIRY_WINDOW_OPTIONS,
        selected_expiry_months=DEFAULT_EXPIRY_WINDOW_MONTHS,
    )


@app.route("/operators", methods=["GET"])
def operators_api():
    """Return operators for a given datatype (AJAX helper)."""
    datatype = request.args.get("datatype", "string")
    ops = OPERATORS.get(datatype, OPERATORS["string"])
    return jsonify(ops)


@app.route("/download-input/<which>", methods=["GET"])
def download_input(which: str):
    """Download File 1..8 currently selected for comparison."""
    if not which.isdigit():
        return redirect(url_for("home"))

    file_index = int(which)
    if file_index < 1 or file_index > MAX_UPLOAD_FILES:
        return redirect(url_for("home"))

    if file_index == 1:
        stored_name = session.get("file1")
        original_name = os.path.basename(session.get("original1", DEFAULT_SAMPLE_FILE1))
        if stored_name:
            stored_path = os.path.join(UPLOAD_FOLDER, stored_name)
            if os.path.exists(stored_path):
                return send_file(stored_path, as_attachment=True, download_name=original_name)

        sample_path = os.path.join(os.path.dirname(__file__), "sample_data", DEFAULT_SAMPLE_FILE1)
        if os.path.exists(sample_path):
            return send_file(sample_path, as_attachment=True, download_name=DEFAULT_SAMPLE_FILE1)
        return redirect(url_for("home"))

    comparison_entries = get_comparison_file_entries()
    comparison_idx = file_index - 2
    if comparison_idx < len(comparison_entries):
        entry = comparison_entries[comparison_idx]
        stored_path = os.path.join(UPLOAD_FOLDER, entry["stored"])
        if os.path.exists(stored_path):
            return send_file(
                stored_path,
                as_attachment=True,
                download_name=os.path.basename(entry["original"]),
            )

    if file_index == 2:
        sample_path = os.path.join(os.path.dirname(__file__), "sample_data", DEFAULT_SAMPLE_FILE2)
        if os.path.exists(sample_path):
            return send_file(sample_path, as_attachment=True, download_name=DEFAULT_SAMPLE_FILE2)

    return redirect(url_for("home"))


@app.route("/compare", methods=["POST"])
def compare():
    """Run the comparison and return a ZIP file with the two result Excel files."""
    fname1 = session.get("file1")
    comparison_entries = get_comparison_file_entries()
    if not fname1 or not comparison_entries:
        return redirect(url_for("order_creator"))

    path1 = os.path.join(UPLOAD_FOLDER, fname1)

    try:
        df1 = read_excel(path1)
    except Exception as exc:  # noqa: BLE001
        return render_template(
            "configure.html",
            cols1=session.get("cols1", []),
            cols2=session.get("cols2", []),
            datatypes=DATATYPES,
            operators_json=json.dumps(OPERATORS),
            original1=session.get("original1", "File 1"),
            original2=session.get("original2", "File 2"),
            comparison_count=len(comparison_entries),
            expiry_options=EXPIRY_WINDOW_OPTIONS,
            selected_expiry_months=DEFAULT_EXPIRY_WINDOW_MONTHS,
            errors=[f"Could not read Excel files: {exc}"],
        )

    selected_expiry_months = request.form.get("expiry_window", str(DEFAULT_EXPIRY_WINDOW_MONTHS))
    valid_expiry_values = {str(value) for value, _label in EXPIRY_WINDOW_OPTIONS}
    if selected_expiry_months not in valid_expiry_values:
        selected_expiry_months = str(DEFAULT_EXPIRY_WINDOW_MONTHS)
    selected_expiry_months_int = int(selected_expiry_months)

    # Parse criteria from the form
    # Fields are submitted as: col1_0, col2_0, datatype_0, operator_0, col1_1, ...
    criteria = []
    index = 0
    while True:
        col1 = request.form.get(f"col1_{index}")
        col2 = request.form.get(f"col2_{index}")
        datatype = request.form.get(f"datatype_{index}")
        operator = request.form.get(f"operator_{index}")
        if col1 is None:
            break
        if col1 and col2 and datatype and operator:
            criteria.append(
                {"col1": col1, "col2": col2, "datatype": datatype, "operator": operator}
            )
        index += 1

    if not criteria:
        cols1 = session.get("cols1", [])
        cols2 = session.get("cols2", [])
        return render_template(
            "configure.html",
            cols1=cols1,
            cols2=cols2,
            datatypes=DATATYPES,
            operators_json=json.dumps(OPERATORS),
            original1=session.get("original1", "File 1"),
            original2=session.get("original2", "File 2"),
            comparison_count=len(comparison_entries),
            expiry_options=EXPIRY_WINDOW_OPTIONS,
            selected_expiry_months=selected_expiry_months_int,
            errors=["Please add at least one comparison criterion."],
        )

    try:
        unmatched_working = df1.copy()
        per_step_results = []

        for entry in comparison_entries:
            if unmatched_working.empty:
                break

            df_compare = read_excel(os.path.join(UPLOAD_FOLDER, entry["stored"]))
            try:
                df_compare = filter_by_expiry_window(df_compare, selected_expiry_months_int)
            except ValueError as exc:
                return render_template(
                    "configure.html",
                    cols1=session.get("cols1", []),
                    cols2=session.get("cols2", []),
                    datatypes=DATATYPES,
                    operators_json=json.dumps(OPERATORS),
                    original1=session.get("original1", "File 1"),
                    original2=session.get("original2", "File 2"),
                    comparison_count=len(comparison_entries),
                    expiry_options=EXPIRY_WINDOW_OPTIONS,
                    selected_expiry_months=selected_expiry_months_int,
                    errors=[f"{entry['original']}: {exc}"],
                )

            missing_col1 = [c["col1"] for c in criteria if c["col1"] not in unmatched_working.columns]
            missing_col2 = [c["col2"] for c in criteria if c["col2"] not in df_compare.columns]
            if missing_col1:
                return render_template(
                    "configure.html",
                    cols1=session.get("cols1", []),
                    cols2=session.get("cols2", []),
                    datatypes=DATATYPES,
                    operators_json=json.dumps(OPERATORS),
                    original1=session.get("original1", "File 1"),
                    original2=session.get("original2", "File 2"),
                    comparison_count=len(comparison_entries),
                    expiry_options=EXPIRY_WINDOW_OPTIONS,
                    selected_expiry_months=selected_expiry_months_int,
                    errors=[f"Missing column(s) in File 1/unmatched data: {', '.join(sorted(set(missing_col1)))}"],
                )
            if missing_col2:
                return render_template(
                    "configure.html",
                    cols1=session.get("cols1", []),
                    cols2=session.get("cols2", []),
                    datatypes=DATATYPES,
                    operators_json=json.dumps(OPERATORS),
                    original1=session.get("original1", "File 1"),
                    original2=session.get("original2", "File 2"),
                    comparison_count=len(comparison_entries),
                    expiry_options=EXPIRY_WINDOW_OPTIONS,
                    selected_expiry_months=selected_expiry_months_int,
                    errors=[
                        f"Missing required comparison column(s) in {entry['original']}: "
                        f"{', '.join(sorted(set(missing_col2)))}"
                    ],
                )

            current_unmatched_input = unmatched_working.copy()
            _matched_df2, next_unmatched = compare_dataframes(
                current_unmatched_input,
                df_compare,
                criteria,
            )

            # Matched output should contain comparison-side rows (File 2 perspective),
            # including columns like Drug Code from the stock file.
            per_step_results.append(
                {
                    "step": len(per_step_results) + 1,
                    "comparison_name": entry["original"],
                    "matched": _matched_df2.reset_index(drop=True),
                    "unmatched": next_unmatched.reset_index(drop=True),
                }
            )

            unmatched_working = next_unmatched

        if per_step_results:
            matched = per_step_results[-1]["matched"]
            unmatched = per_step_results[-1]["unmatched"]
        else:
            matched = pd.DataFrame(columns=df1.columns)
            unmatched = df1.copy()
    except Exception as exc:  # noqa: BLE001
        return render_template(
            "configure.html",
            cols1=session.get("cols1", []),
            cols2=session.get("cols2", []),
            datatypes=DATATYPES,
            operators_json=json.dumps(OPERATORS),
            original1=session.get("original1", "File 1"),
            original2=session.get("original2", "File 2"),
            comparison_count=len(comparison_entries),
            expiry_options=EXPIRY_WINDOW_OPTIONS,
            selected_expiry_months=selected_expiry_months_int,
            errors=[f"Could not compare Excel files: {exc}"],
        )

    # Write both result DataFrames into an in-memory ZIP file
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    def _step_file_label(step_number: int, comparison_name: str) -> str:
        base = os.path.basename(comparison_name)
        root, _ext = os.path.splitext(base)
        safe_root = "".join(ch if ch.isalnum() or ch in {"-", "_"} else "_" for ch in root)
        safe_root = safe_root.strip("_") or f"file{step_number + 1}"
        return f"step{step_number}_{safe_root}"

    def _safe_file_root(filename: str, fallback: str) -> str:
        base = os.path.basename(filename)
        root, _ext = os.path.splitext(base)
        safe_root = "".join(ch if ch.isalnum() or ch in {"-", "_"} else "_" for ch in root)
        return safe_root.strip("_") or fallback

    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
        used_names = set()

        def unique_name(filename: str) -> str:
            if filename not in used_names:
                used_names.add(filename)
                return filename

            root, ext = os.path.splitext(filename)
            counter = 2
            while True:
                candidate = f"{root}_{counter}{ext}"
                if candidate not in used_names:
                    used_names.add(candidate)
                    return candidate
                counter += 1

        # Step-wise outputs for each pairwise comparison.
        for step_result in per_step_results:
            label = _step_file_label(step_result["step"], step_result["comparison_name"])
            safe_root = _safe_file_root(step_result["comparison_name"], f"file{step_result['step'] + 1}")
            for df, name in [
                (step_result["matched"], f"{safe_root}_Order.xlsx"),
                (step_result["unmatched"], f"unmatched_{label}_{timestamp}.xlsx"),
            ]:
                excel_buffer = io.BytesIO()
                with pd.ExcelWriter(excel_buffer, engine="openpyxl") as writer:
                    df.to_excel(writer, index=False)
                zf.writestr(unique_name(name), excel_buffer.getvalue())

        # Backward-compatible aliases: final step result.
        for df, name in [
            (matched, f"matched_{timestamp}.xlsx"),
            (unmatched, f"unmatched_{timestamp}.xlsx"),
        ]:
            excel_buffer = io.BytesIO()
            with pd.ExcelWriter(excel_buffer, engine="openpyxl") as writer:
                df.to_excel(writer, index=False)
            zf.writestr(unique_name(name), excel_buffer.getvalue())

    zip_buffer.seek(0)
    return send_file(
        zip_buffer,
        mimetype="application/zip",
        as_attachment=True,
        download_name=f"comparison_results_{timestamp}.zip",
    )


if __name__ == "__main__":
    app.run(debug=False)
