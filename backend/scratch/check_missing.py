import pandas as pd
import os

INPUT_FILE = "e:/Internship/PocketFM/scraped_data.xlsx"

print(f"Loading {INPUT_FILE}...")
df = pd.read_excel(INPUT_FILE)

# Focus on the batch range 4003 to 4102 (Index 4001 to 4101)
# Excel Row 2 is Index 0
start_idx = 4001
end_idx = 4101
subset = df.iloc[start_idx:end_idx+1]

target_cols = [
    "GoodReads_Series_URL", 
    "Book1_Rating", 
    "Book1_Num_Ratings"
]

missing_count = 0
missing_indices = []

for index, row in subset.iterrows():
    is_missing = False
    for col in target_cols:
        v = str(row.get(col, "N/A")).strip()
        if pd.isna(row.get(col)) or v == "N/A" or v == "" or v == "0" or v == "0.0":
            is_missing = True
            break
    
    if is_missing:
        missing_count += 1
        missing_indices.append(index + 2) # Excel Row number

print(f"Total rows in batch: {len(subset)}")
print(f"Missing/Incomplete data: {missing_count}")
if missing_indices:
    print(f"Excel Rows to retry: {missing_indices}")
