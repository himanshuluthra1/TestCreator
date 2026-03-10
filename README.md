# TestCreator

Sample files for quick comparison are included in this repository:

- sample_data/sample_file1.xlsx
- sample_data/sample_file2.xlsx

These are product-inventory files.

Quick test steps:

1. Start the app from project root: python3 app.py
2. Open http://127.0.0.1:5000 in your browser.
3. Keep file inputs empty and submit to use defaults, or upload sample_data/sample_file1.xlsx and sample_data/sample_file2.xlsx manually.
4. Add criteria using this mapping:
	- ProductName -> ItemName (string, contains)
	- Category -> CategoryType (string, eq)
	- Warehouse -> Location (string, eq)
	- ExpectedQty -> StockQty (number, eq)
5. Run compare and download the ZIP result.

Expected behavior:

- Matched output includes inventory rows from File 2 for Wireless Mouse and Laptop Stand.
- Unmatched output includes products from File 1 where summed StockQty in File 2 does not satisfy ExpectedQty.

Number comparison rule:

- If a number criterion is used together with other criteria, the app first filters rows in File 2 by the non-number criteria.
- Then it sums the matching values from the number column in File 2 and compares this total with the File 1 number value using the selected operator (eq, gt, lt, etc.).