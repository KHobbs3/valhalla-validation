import numpy as np
import sys
import pandas as pd
import pyspark as ps

cma = sys.argv[1]

# Read Data ----------------------------
# Filtered db idx coordinates as geodataframe
db = pd.read_csv("data/idx_dbs_coords_CMA.csv")

# h5 data
print("Read HDF data.")
store = pd.HDFStore("../matrices/db-pairs-20km-cycling-final.h5")


# Filter ----------------------------
print("Subsetting ttms according to unique filtered DBUIDs.")

def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]
        
        
# filter ttms by unique db coord indices
final = []
print(cma)

# subset dataframe
temp = db.loc[db.CMANAME == cma]

print(len(temp.idx.unique()))
# grab indices from ttm store
i = 1
for chunk in chunks(temp, 10):
    print(f"{i} of {len(temp)//10}")
    row_idx = chunk.idx.unique().tolist()
    ttm = store.select("network", where=f"idx_src={row_idx}")
    ttm.reset_index(inplace=True)


    final.append(ttm)
    i += 1

# Export data ----------------------------
    # if i % 1000 == 0:
    #     print(f"Exporting {cma}...")
    #     pd.concat(final).to_csv(f"output/ttms-filtered-{cma}-{i}.csv", index = False)

    #     # empty list of dfs after every 1000 chunks and restart for memory
    #     final = []

ps.pandas.concat(final)#.to_csv(f"output/ttms-filtered-{cma}-{i}.csv", index = False)


# double merge to get dbuid of src and dst
print("merge 1...")
merge1 = db.merge(cma_df, how = "right",
         left_on = "idx",
         right_on = "idx_src")

print("merge 2...")
merge2 = db[['idx','dbuid']].merge(merge1, how = "right",
                           left_on = "idx",
                           right_on = "idx_dst",
                           suffixes = ["_source", "_dest"]
                          )

# export
dur = merge2.duration/60
dur.to_pandas().to_csv(f"output/summary/{c}-durations.csv", index = False) 