import pandas as pd

file_numbers = [20, 25, 30, 35, 40, 45]

filename_default = "Segmented_PCT_NUMBER_Segments.csv"

spareDescriptions = True

def merge_file(filename):

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

    try:
        pct_30.insert(13, "Description_new", "")
    except:
        pass

    try:
        pct_30.insert(14, "imagesPath", "")
    except:
        pass

    print(pct_30.columns)

    for mergingindex, mergingrow in pct_30.iterrows():
        description = mergingrow["Description"]
        if spareDescriptions is False:
            pct_30.at[mergingindex, 'Description_new'] = mergingrow["Description"]

        photo = None
        photo_description = None
        for biggerindex, biggerrow in pct_50.iterrows():
            if biggerrow["Description"] in description:
                if spareDescriptions is False:
                    pct_30.at[mergingindex, 'Description_new'] = pct_30.at[mergingindex, 'Description_new'].replace(biggerrow["Description"], biggerrow["Description_new"]).replace(",", ";")
                    pct_30.at[mergingindex, 'Description'] = pct_30.at[mergingindex, 'Description'].replace(",", ";")
                if photo is None:
                    photo = biggerrow["imagesPath"]
                    photo_description = biggerrow["Landmarks"]
                pct_30.at[mergingindex, 'imagesPath'] = photo
                pct_30.at[mergingindex, 'Landmarks'] = photo_description

    try:
        pct_30 = pct_30.drop('index', axis=1)
    except:
        pass

    try:
        pct_30 = pct_30.drop('level_0', axis=1)
    except:
        pass

    pct_30.to_csv("merged/"+filename, index=False)

for nm in file_numbers:
    merge_file(filename_default.replace("NUMBER", str(nm)))