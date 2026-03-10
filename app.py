"""Excel Comparison Web Application.

Upload two Excel files, configure column-level comparison criteria
(column mapping, datatype, operator), then download:
  - matched.xlsx   – rows from file 2 that matched at least one row in file 1
  - unmatched.xlsx – rows from file 1 that did not match any row in file 2
"""

import io
import json
import os
import uuid
import zipfile
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


def allowed_file(filename: str) -> bool:
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXTENSIONS


def safe_filename(filename: str) -> str:
    """Return a safe version of the filename with a unique prefix."""
    ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else "xlsx"
    return f"{uuid.uuid4().hex}.{ext}"


def read_excel(path: str) -> pd.DataFrame:
    """Read an Excel file and return a DataFrame with all values as strings
    (raw) in addition to a parsed version."""
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
        return left.str.contains(str(right_scalar), na=False, case=False)
    if operator == "startswith":
        return left.str.startswith(str(right_scalar))
    if operator == "endswith":
        return left.str.endswith(str(right_scalar))
    raise ValueError(f"Unknown operator: {operator}")


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

    for i1, row1 in df1_cast.iterrows():
        # Build a combined boolean mask for df2 rows that match row1 on ALL criteria
        mask = pd.Series([True] * len(df2_cast), index=df2_cast.index)
        for crit in criteria:
            col1, col2, operator = crit["col1"], crit["col2"], crit["operator"]
            row_mask = apply_operator(df2_cast[col2], row1[col1], operator)
            mask = mask & row_mask

        if mask.any():
            matched_df1_indices.add(i1)
            matched_df2_indices.update(df2_cast.index[mask].tolist())

    unmatched_df1_indices = [i for i in df1.index if i not in matched_df1_indices]

    matched = df2.loc[sorted(matched_df2_indices)].reset_index(drop=True)
    unmatched = df1.loc[unmatched_df1_indices].reset_index(drop=True)

    return matched, unmatched


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------


@app.route("/", methods=["GET"])
def index():
    return render_template("index.html")


@app.route("/upload", methods=["POST"])
def upload():
    """Accept two Excel files, store them, forward to configure page."""
    file1 = request.files.get("file1")
    file2 = request.files.get("file2")

    errors = []
    if not file1 or file1.filename == "":
        errors.append("Please select the first Excel file.")
    elif not allowed_file(file1.filename):
        errors.append("File 1 must be an Excel file (.xlsx or .xls).")

    if not file2 or file2.filename == "":
        errors.append("Please select the second Excel file.")
    elif not allowed_file(file2.filename):
        errors.append("File 2 must be an Excel file (.xlsx or .xls).")

    if errors:
        return render_template("index.html", errors=errors)

    fname1 = safe_filename(file1.filename)
    fname2 = safe_filename(file2.filename)
    path1 = os.path.join(UPLOAD_FOLDER, fname1)
    path2 = os.path.join(UPLOAD_FOLDER, fname2)
    file1.save(path1)
    file2.save(path2)

    try:
        df1 = read_excel(path1)
        df2 = read_excel(path2)
    except Exception as exc:  # noqa: BLE001
        return render_template("index.html", errors=[f"Could not read Excel file: {exc}"])

    session["file1"] = fname1
    session["file2"] = fname2
    session["original1"] = file1.filename
    session["original2"] = file2.filename
    session["cols1"] = list(df1.columns)
    session["cols2"] = list(df2.columns)

    return redirect(url_for("configure"))


@app.route("/configure", methods=["GET"])
def configure():
    cols1 = session.get("cols1", [])
    cols2 = session.get("cols2", [])
    if not cols1 or not cols2:
        return redirect(url_for("index"))

    return render_template(
        "configure.html",
        cols1=cols1,
        cols2=cols2,
        datatypes=DATATYPES,
        operators_json=json.dumps(OPERATORS),
        original1=session.get("original1", "File 1"),
        original2=session.get("original2", "File 2"),
    )


@app.route("/operators", methods=["GET"])
def operators_api():
    """Return operators for a given datatype (AJAX helper)."""
    datatype = request.args.get("datatype", "string")
    ops = OPERATORS.get(datatype, OPERATORS["string"])
    return jsonify(ops)


@app.route("/compare", methods=["POST"])
def compare():
    """Run the comparison and return a ZIP file with the two result Excel files."""
    fname1 = session.get("file1")
    fname2 = session.get("file2")
    if not fname1 or not fname2:
        return redirect(url_for("index"))

    path1 = os.path.join(UPLOAD_FOLDER, fname1)
    path2 = os.path.join(UPLOAD_FOLDER, fname2)

    try:
        df1 = read_excel(path1)
        df2 = read_excel(path2)
    except Exception as exc:  # noqa: BLE001
        return render_template(
            "configure.html",
            cols1=session.get("cols1", []),
            cols2=session.get("cols2", []),
            datatypes=DATATYPES,
            operators_json=json.dumps(OPERATORS),
            original1=session.get("original1", "File 1"),
            original2=session.get("original2", "File 2"),
            errors=[f"Could not read Excel files: {exc}"],
        )

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
            errors=["Please add at least one comparison criterion."],
        )

    matched, unmatched = compare_dataframes(df1, df2, criteria)

    # Write both result DataFrames into an in-memory ZIP file
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, "w", zipfile.ZIP_DEFLATED) as zf:
        for df, name in [
            (matched, f"matched_{timestamp}.xlsx"),
            (unmatched, f"unmatched_{timestamp}.xlsx"),
        ]:
            excel_buffer = io.BytesIO()
            with pd.ExcelWriter(excel_buffer, engine="openpyxl") as writer:
                df.to_excel(writer, index=False)
            zf.writestr(name, excel_buffer.getvalue())

    zip_buffer.seek(0)
    return send_file(
        zip_buffer,
        mimetype="application/zip",
        as_attachment=True,
        download_name=f"comparison_results_{timestamp}.zip",
    )


if __name__ == "__main__":
    app.run(debug=True)
