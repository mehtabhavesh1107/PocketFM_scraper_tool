import pandas as pd
import os
import glob

def get_latest_file(pattern):
    files = glob.glob(pattern)
    if not files: return "scraped_data.xlsx"
    return max(files, key=os.path.getctime)

latest = get_latest_file("scraped_data*.xlsx")
print(f"Loading {latest} to refresh labels...")

df = pd.read_excel(latest)

# Ensure the column can accept "True" labels (cast to object)
df['Part of a Series?'] = df['Part of a Series?'].astype(object)

# Logic: If Series Name is present and not N/A, set "Part of a Series?" to True
mask = (df['Series Name'].notna()) & (df['Series Name'] != 'N/A')
df.loc[mask, 'Part of a Series?'] = True
df.loc[~mask, 'Part of a Series?'] = False

output = "scraped_data_labeled.xlsx"
df.to_excel(output, index=False)
print(f"SUCCESS: Labeled file created at {output}")
