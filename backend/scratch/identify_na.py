import pandas as pd

INPUT_FILE = "e:/Internship/PocketFM/scraped_data.xlsx"
df = pd.read_excel(INPUT_FILE)

# Range 4003 to 4569 (Indices 4001 to 4567)
subset = df.iloc[4001:4568]

target_cols = ["GoodReads_Series_URL", "Book1_Rating", "Book1_Num_Ratings"]
missing_rows = []

for index, row in subset.iterrows():
    is_missing = False
    for col in target_cols:
        val = str(row.get(col, "N/A")).strip()
        if pd.isna(row.get(col)) or val == "N/A" or val == "" or val == "0" or val == "0.0":
            is_missing = True
            break
    if is_missing:
        missing_rows.append(index + 2) # Excel row number

print(f"Total rows checked: {len(subset)}")
print(f"Rows with N/A or incomplete data: {len(missing_rows)}")
print(f"Example rows to fix: {missing_rows[:20]}...")
