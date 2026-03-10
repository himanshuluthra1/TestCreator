"""Tests for the Excel comparison logic in app.py."""

import io
import zipfile

import pandas as pd
import pytest

from app import (
    app,
    cast_series,
    apply_operator,
    apply_scalar_operator,
    compare_dataframes,
    OPERATORS,
    DATATYPES,
)


# ---------------------------------------------------------------------------
# Unit tests – cast_series
# ---------------------------------------------------------------------------


def test_cast_series_string():
    s = pd.Series(["  Alice ", "Bob", " Charlie "])
    result = cast_series(s, "string")
    assert list(result) == ["Alice", "Bob", "Charlie"]


def test_cast_series_number():
    s = pd.Series(["1", "2.5", "abc"])
    result = cast_series(s, "number")
    assert result[0] == 1.0
    assert result[1] == 2.5
    assert pd.isna(result[2])


def test_cast_series_date():
    s = pd.Series(["2024-01-15", "2023-06-01", "not-a-date"])
    result = cast_series(s, "date")
    assert pd.notna(result[0])
    assert pd.notna(result[1])
    assert pd.isna(result[2])


def test_cast_series_boolean():
    s = pd.Series(["true", "1", "yes", "false", "0", "no", "maybe"])
    result = cast_series(s, "boolean")
    assert result[0] is True
    assert result[1] is True
    assert result[2] is True
    assert result[3] is False
    assert result[4] is False
    assert result[5] is False
    assert pd.isna(result[6])


# ---------------------------------------------------------------------------
# Unit tests – apply_operator
# ---------------------------------------------------------------------------


def test_apply_operator_eq():
    s = pd.Series(["Alice", "Bob", "Alice"])
    mask = apply_operator(s, "Alice", "eq")
    assert list(mask) == [True, False, True]


def test_apply_operator_ne():
    s = pd.Series(["Alice", "Bob"])
    mask = apply_operator(s, "Alice", "ne")
    assert list(mask) == [False, True]


def test_apply_operator_gt_number():
    s = pd.to_numeric(pd.Series(["1", "5", "10"]))
    mask = apply_operator(s, 5, "gt")
    assert list(mask) == [False, False, True]


def test_apply_operator_gte():
    s = pd.to_numeric(pd.Series(["1", "5", "10"]))
    mask = apply_operator(s, 5, "gte")
    assert list(mask) == [False, True, True]


def test_apply_operator_lt():
    s = pd.to_numeric(pd.Series(["1", "5", "10"]))
    mask = apply_operator(s, 5, "lt")
    assert list(mask) == [True, False, False]


def test_apply_operator_lte():
    s = pd.to_numeric(pd.Series(["1", "5", "10"]))
    mask = apply_operator(s, 5, "lte")
    assert list(mask) == [True, True, False]


def test_apply_operator_contains():
    s = pd.Series(["hello world", "foo bar", "Hello Python"])
    mask = apply_operator(s, "hello", "contains")
    assert list(mask) == [True, False, True]


def test_apply_operator_startswith():
    s = pd.Series(["hello world", "foo bar"])
    mask = apply_operator(s, "hello", "startswith")
    assert list(mask) == [True, False]


def test_apply_operator_endswith():
    s = pd.Series(["hello world", "foo bar"])
    mask = apply_operator(s, "world", "endswith")
    assert list(mask) == [True, False]


def test_apply_operator_unknown_raises():
    s = pd.Series(["a"])
    with pytest.raises(ValueError, match="Unknown operator"):
        apply_operator(s, "a", "not_an_operator")


def test_apply_scalar_operator_eq():
    assert apply_scalar_operator(10, 10, "eq") is True
    assert apply_scalar_operator(10, 12, "eq") is False


# ---------------------------------------------------------------------------
# Unit tests – compare_dataframes
# ---------------------------------------------------------------------------


def _make_df(data: dict) -> pd.DataFrame:
    return pd.DataFrame({k: [str(v) for v in vals] for k, vals in data.items()})


def test_compare_no_criteria():
    df1 = _make_df({"A": [1, 2]})
    df2 = _make_df({"B": [3, 4]})
    matched, unmatched = compare_dataframes(df1, df2, [])
    assert matched.empty
    assert len(unmatched) == 2


def test_compare_string_eq_all_match():
    df1 = _make_df({"Name": ["Alice", "Bob"]})
    df2 = _make_df({"FullName": ["Alice", "Bob", "Charlie"]})
    criteria = [{"col1": "Name", "col2": "FullName", "datatype": "string", "operator": "eq"}]
    matched, unmatched = compare_dataframes(df1, df2, criteria)
    assert set(matched["FullName"]) == {"Alice", "Bob"}
    assert unmatched.empty


def test_compare_string_eq_partial_match():
    df1 = _make_df({"Name": ["Alice", "Dave"]})
    df2 = _make_df({"FullName": ["Alice", "Bob", "Charlie"]})
    criteria = [{"col1": "Name", "col2": "FullName", "datatype": "string", "operator": "eq"}]
    matched, unmatched = compare_dataframes(df1, df2, criteria)
    assert list(matched["FullName"]) == ["Alice"]
    assert list(unmatched["Name"]) == ["Dave"]


def test_compare_number_gte():
    df1 = _make_df({"Score": [5]})
    df2 = _make_df({"Points": [3, 5, 8]})
    criteria = [{"col1": "Score", "col2": "Points", "datatype": "number", "operator": "gte"}]
    matched, unmatched = compare_dataframes(df1, df2, criteria)
    # Numeric semantics are File1 OP File2: Score >= Points.
    assert set(matched["Points"]) == {"3", "5"}
    assert unmatched.empty  # df1 row matched


def test_compare_multiple_criteria():
    df1 = pd.DataFrame({"Name": ["Alice"], "Age": ["55"]})
    df2 = pd.DataFrame({"FullName": ["Alice", "Alice", "Bob"], "Years": ["30", "25", "30"]})
    criteria = [
        {"col1": "Name", "col2": "FullName", "datatype": "string", "operator": "eq"},
        {"col1": "Age", "col2": "Years", "datatype": "number", "operator": "eq"},
    ]
    matched, unmatched = compare_dataframes(df1, df2, criteria)
    # Number criterion is evaluated against SUM(Years) of rows matching Name=Alice.
    assert len(matched) == 2
    assert set(matched["FullName"]) == {"Alice"}
    assert set(matched["Years"]) == {"30", "25"}
    assert unmatched.empty


def test_compare_number_sum_with_other_criteria_no_match():
    df1 = pd.DataFrame({"Product": ["Mouse"], "Warehouse": ["Delhi"], "ExpectedQty": ["100"]})
    df2 = pd.DataFrame(
        {
            "Item": ["Mouse Black", "Mouse White", "Mouse Wireless"],
            "Location": ["Delhi", "Delhi", "Delhi"],
            "Qty": ["40", "50", "15"],
        }
    )
    criteria = [
        {"col1": "Product", "col2": "Item", "datatype": "string", "operator": "contains"},
        {"col1": "Warehouse", "col2": "Location", "datatype": "string", "operator": "eq"},
        {"col1": "ExpectedQty", "col2": "Qty", "datatype": "number", "operator": "eq"},
    ]
    matched, unmatched = compare_dataframes(df1, df2, criteria)
    assert matched.empty
    assert list(unmatched["Product"]) == ["Mouse"]


def test_compare_number_sum_lte_direction_regression():
    """Regression: ExpectedQty <= SUM(StockQty) should not match 80 <= 75."""
    df1 = pd.DataFrame(
        {
            "ProductName": ["USB Keyboard"],
            "Category": ["Accessories"],
            "Warehouse": ["Delhi"],
            "ExpectedQty": ["80"],
        }
    )
    df2 = pd.DataFrame(
        {
            "ItemName": ["USB Keyboard Basic", "USB Keyboard Pro"],
            "CategoryType": ["Accessories", "Accessories"],
            "Location": ["Delhi", "Delhi"],
            "StockQty": ["30", "45"],
        }
    )
    criteria = [
        {"col1": "ProductName", "col2": "ItemName", "datatype": "string", "operator": "contains"},
        {"col1": "Category", "col2": "CategoryType", "datatype": "string", "operator": "eq"},
        {"col1": "Warehouse", "col2": "Location", "datatype": "string", "operator": "eq"},
        {"col1": "ExpectedQty", "col2": "StockQty", "datatype": "number", "operator": "lte"},
    ]
    matched, unmatched = compare_dataframes(df1, df2, criteria)
    assert matched.empty
    assert list(unmatched["ProductName"]) == ["USB Keyboard"]


def test_compare_no_match():
    df1 = _make_df({"Name": ["Xavier"]})
    df2 = _make_df({"FullName": ["Alice", "Bob"]})
    criteria = [{"col1": "Name", "col2": "FullName", "datatype": "string", "operator": "eq"}]
    matched, unmatched = compare_dataframes(df1, df2, criteria)
    assert matched.empty
    assert list(unmatched["Name"]) == ["Xavier"]


# ---------------------------------------------------------------------------
# Integration tests – Flask routes
# ---------------------------------------------------------------------------


@pytest.fixture
def client(tmp_path):
    app.config["TESTING"] = True
    app.config["SECRET_KEY"] = "test-secret"
    # Redirect uploads/outputs to tmp dirs
    import app as app_module
    app_module.UPLOAD_FOLDER = str(tmp_path / "uploads")
    app_module.OUTPUT_FOLDER = str(tmp_path / "outputs")
    (tmp_path / "uploads").mkdir()
    (tmp_path / "outputs").mkdir()
    with app.test_client() as client:
        with app.app_context():
            yield client


def _make_excel_bytes(data: dict) -> bytes:
    buf = io.BytesIO()
    pd.DataFrame(data).to_excel(buf, index=False)
    return buf.getvalue()


def test_index_get(client):
    resp = client.get("/")
    assert resp.status_code == 200
    assert b"Operations Hub" in resp.data
    assert b"Order Creator" in resp.data
    assert b"Stock Checker" in resp.data


def test_order_creator_page_get(client):
    resp = client.get("/order-creator")
    assert resp.status_code == 200
    assert b"Excel File Comparator" in resp.data


def test_stock_checker_page_get(client):
    resp = client.get("/stock-checker")
    assert resp.status_code == 200
    assert b"Stock Checker" in resp.data


def test_upload_missing_files(client):
    resp = client.post("/upload", data={})
    assert resp.status_code == 200
    assert b"Please select" in resp.data


def test_upload_wrong_extension(client):
    data = {
        "file1": (io.BytesIO(b"data"), "file1.csv"),
        "file2": (io.BytesIO(b"data"), "file2.csv"),
    }
    resp = client.post("/upload", data=data, content_type="multipart/form-data")
    assert resp.status_code == 200
    assert b"Excel file" in resp.data


def test_full_compare_flow(client):
    """Upload two Excel files and run a comparison through the web interface."""
    excel1 = _make_excel_bytes({"Name": ["Alice", "Bob", "Dave"], "Score": [90, 75, 60]})
    excel2 = _make_excel_bytes({"FullName": ["Alice", "Bob", "Charlie"], "Points": [90, 80, 70]})

    with client.session_transaction() as sess:
        pass  # ensure session started

    # Step 1: Upload
    upload_resp = client.post(
        "/upload",
        data={
            "file1": (io.BytesIO(excel1), "file1.xlsx"),
            "file2": (io.BytesIO(excel2), "file2.xlsx"),
        },
        content_type="multipart/form-data",
        follow_redirects=True,
    )
    assert upload_resp.status_code == 200
    assert b"Configure" in upload_resp.data or b"col" in upload_resp.data.lower()

    # Step 2: Compare
    compare_resp = client.post(
        "/compare",
        data={
            "col1_0": "Name",
            "col2_0": "FullName",
            "datatype_0": "string",
            "operator_0": "eq",
        },
        follow_redirects=True,
    )
    assert compare_resp.status_code == 200
    assert compare_resp.content_type == "application/zip"

    # Inspect the ZIP contents
    zf = zipfile.ZipFile(io.BytesIO(compare_resp.data))
    names = zf.namelist()
    assert any("matched" in n for n in names)
    assert any("unmatched" in n for n in names)

    matched_bytes = zf.read(next(n for n in names if "matched" in n))
    unmatched_bytes = zf.read(next(n for n in names if "unmatched" in n))

    matched_df = pd.read_excel(io.BytesIO(matched_bytes))
    unmatched_df = pd.read_excel(io.BytesIO(unmatched_bytes))

    assert set(matched_df["FullName"]) == {"Alice", "Bob"}
    assert list(unmatched_df["Name"]) == ["Dave"]


def test_download_input_file_after_upload(client):
    excel1 = _make_excel_bytes({"Name": ["Alice"]})
    excel2 = _make_excel_bytes({"FullName": ["Alice"]})

    client.post(
        "/upload",
        data={
            "file1": (io.BytesIO(excel1), "file1.xlsx"),
            "file2": (io.BytesIO(excel2), "file2.xlsx"),
        },
        content_type="multipart/form-data",
    )

    resp = client.get("/download-input/1")
    assert resp.status_code == 200
    assert "attachment" in resp.headers.get("Content-Disposition", "")


def test_compare_no_criteria_shows_error(client):
    """Submitting /compare without criteria shows an error."""
    with client.session_transaction() as sess:
        sess["file1"] = "dummy1.xlsx"
        sess["file2"] = "dummy2.xlsx"
        sess["cols1"] = ["A"]
        sess["cols2"] = ["B"]
        sess["original1"] = "f1.xlsx"
        sess["original2"] = "f2.xlsx"

    # Need real files for this path – use real excel files
    import app as app_module
    excel1 = _make_excel_bytes({"A": ["x"]})
    excel2 = _make_excel_bytes({"B": ["y"]})
    path1 = str(app_module.UPLOAD_FOLDER + "/dummy1.xlsx")
    path2 = str(app_module.UPLOAD_FOLDER + "/dummy2.xlsx")
    with open(path1, "wb") as f:
        f.write(excel1)
    with open(path2, "wb") as f:
        f.write(excel2)

    resp = client.post("/compare", data={})
    assert resp.status_code == 200
    assert b"at least one" in resp.data


def test_operators_api(client):
    resp = client.get("/operators?datatype=number")
    assert resp.status_code == 200
    data = resp.get_json()
    ops = [op[0] for op in data]
    assert "eq" in ops
    assert "gt" in ops
    assert "lt" in ops


def test_operators_api_string(client):
    resp = client.get("/operators?datatype=string")
    data = resp.get_json()
    ops = [op[0] for op in data]
    assert "contains" in ops
    assert "startswith" in ops


# ---------------------------------------------------------------------------
# Constant tests
# ---------------------------------------------------------------------------


def test_all_datatypes_have_operators():
    for dtype, _ in DATATYPES:
        assert dtype in OPERATORS, f"No operators defined for datatype '{dtype}'"
        assert len(OPERATORS[dtype]) > 0
