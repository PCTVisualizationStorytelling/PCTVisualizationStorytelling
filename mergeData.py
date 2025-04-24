import pandas as pd

filename = "Segmented_PCT_45_Segments.csv"

# Load the files
pct_30 = pd.read_csv(filename)
pct_50 = pd.read_csv("Segmented_PCT_50_Segments.csv")

# Clean up column names (remove leading/trailing whitespace)
pct_30.columns = pct_30.columns.str.strip()
pct_50.columns = pct_50.columns.str.strip()

print("Columns")
print(pct_30.columns.difference(pct_50.columns))
print(pct_50.columns.difference(pct_30.columns))
print()

try:
    pct_30 = pct_30.reset_index()  # make sure indexes pair with number of rows
except:
    pass

try:
    pct_50 = pct_50.reset_index()  # make sure indexes pair with number of rows
except:
    pass

merged = pd.merge(
    pct_30,
    pct_50,
    on=["Start Longitude", "Start Latitude"]
)

print(merged)

last_index = 0
merged_rows = []
for index, row in merged.iterrows():
    index_y = row["index_y"]
    collection = []
    photo = None

    df1 = pct_50.iloc[last_index:index_y]
    for mergingindex, mergingrow in df1.iterrows():
        collection.append(mergingrow["Description"].strip())
        if photo is None:
            photo = mergingrow["imagesPath"]

    print(df1)
    print(collection)
    if len(collection) > 0:
        merged_rows.append({"rows": collection, "photo": photo})

    last_index = index_y

collection = []
photo = None
df1 = pct_50.iloc[last_index:]
for mergingindex, mergingrow in df1.iterrows():
    collection.append(mergingrow["Description"].strip())
    if photo is None:
        photo = mergingrow["imagesPath"]

merged_rows.append({"rows": collection, "photo": photo})

print()
print(len(merged_rows))
print(merged_rows)

for index, row in pct_30.iterrows():
    pct_30.at[index, 'Description'] = " ".join(merged_rows[index]["rows"])
    pct_30.at[index, 'imagesPath'] = merged_rows[index]["photo"]

try:
    pct_30 = pct_30.drop('index', axis=1)
except:
    pass

try:
    pct_30 = pct_30.drop('level_0', axis=1)
except:
    pass

pct_30.to_csv("merged/"+filename, index=False)