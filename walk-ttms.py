"""
CYCLING TTM DURATION SUMMARY STATISTICS.

1. Read cycling ttm hstore file and idx concordance file filtered by CMA.
2. Filter ttms according to unique idices of each CMA.
3. Double merge ttm subset with idx concordance file to get dbuids (not neccessary if only looking for dur).
4. Subset durations only and convert to minutes (/60). Append dur to dataframe with CMA name.
5. Visualize and save boxplot as png.
"""

import numpy as np
import pandas as pd
from pyspark.pandas.config import set_option, reset_option
import pyspark as ps
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import random

# Set up
set_option("compute.ops_on_diff_frames", True)

cma = [
                "Vancouver",
                "Montréal",
                "Calgary",
                "Ottawa-Gatineau",
                "Toronto"
]

# initialize empty dataframe
#ttm_full = ps.pandas.DataFrame(columns = cma)
ttm_full = pd.DataFrame(columns = ['empt'])

# 1. Read Data ----------------------------
# Filtered db idx coordinates as geodataframe
db = pd.read_csv("data/idx_dbs_coords_CMA.csv")

# h5 data
print("Read HDF data.")
store = pd.HDFStore("../matrices/db-pairs-5km-walking-final.h5")


# 2. Filter ----------------------------
print("Subsetting ttms according to unique filtered DBUIDs.")

def chunks(lst, n):  
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]
        
        
# filter ttms by unique db coord indices

for c in cma:
        final = []
        print(c)

        # randomly subset dataframe
        # temp = db.loc[db.CMANAME == c].iloc[0:500] # TODO: change to 1000, set to 100 for testing purposes
        temp = db.loc[db.CMANAME == c].reset_index().drop(columns = 'index')
        temp = temp.iloc[random.sample(range(len(temp)), 500)]

        # grab indices from ttm store
        i = 1
        for chunk in chunks(temp, 10):
                print(f"chunk {i} of {len(temp)//10}")
                row_idx = chunk.idx.unique().tolist()
                ttm = store.select("network", where=f"idx_src={row_idx}")
                ttm.reset_index(inplace=True)

                ttmps = ps.pandas.from_pandas(ttm)  

                final.append(ttmps)
                i += 1

        ttm_city = ps.pandas.concat(final)
        print("printing chunk dataframe",ttm_city.head(3))


    # 3. Merge data -------------------
        # # double merge to get dbuid of src and dst
        # print("merge 1...")
        # merge1 = db.merge(ttm_city, how = "right",
        #         left_on = "idx",
        #         right_on = "idx_src")

        # print("merge 2...")
        # merge2 = db[['idx','dbuid']].merge(merge1, how = "right",
        #                           left_on = "idx",
        #                           right_on = "idx_dst",
        #                           suffixes = ["_dest", "_source"]
        #                           )

    # 4. Clean durations and subset --------------
        # convert to min & add to dataframe
        # print("converting to minutes...")
        print("filtering to under 60 min, dropping NaNs and subsetting 1000...")
        ttm_city.duration = ttm_city.duration/60
        ttm_city = ttm_city.loc[ttm_city.duration <=60].dropna()#.iloc[random.sample(range(len(temp)+1), 1250)]

        dur = ttm_city.duration
 

        # Create pyspark pandas dataframe of each city
        print("appending to ps dataframe...")
        #ttseries = ps.pandas.Series({c:dur.tolist()})
        #print(ttseries.head())
        clist = [c]
        tt_series = pd.DataFrame({c:dur.to_list()}, columns = clist)
        # tt_series = tt_series[tt_series<=60].dropna()
        print("printing ttseries", len(tt_series))
        print(tt_series)
        ttm_full = pd.concat([ttm_full, tt_series], axis = 1)
        print(ttm_full.head())

# 5. Seaborn Boxplot ---------------
plt.figure(figsize=(10,8))
sns.set_style("white", {'axes.grid':False})

ttm_full.drop(columns = 'empt', inplace= True)
cma_stats = sns.boxplot(data = ttm_full, showmeans = True, meanprops = {"marker":"o",
                                                               "markerfacecolor":"white",
                                                               "markeredgecolor":"black",
                                                               "markersize":"8"}, palette="Blues")

print("Plotting...")
medians = ttm_full.median().apply(lambda x: round(x, 1)).values
medians = pd.Series(medians)
vertical_offset = ttm_full.median().median() * 0.1
for xtick in cma_stats.get_xticks():
    cma_stats.text(xtick, medians[xtick] + vertical_offset, medians[xtick],
                  horizontalalignment = 'center', size = 'small', color = '#000000', weight="bold")
plt.xlabel("Census Metropolitan Areas", size = 14, fontdict={'weight':'bold'})
plt.ylabel("Travel times in minutes", size =14,fontdict={'weight':'bold'})
#ind = 0
#for tick in range(len(cma_stats.get_xticklabels())):
    #cma_stats.text(tick+.2, m1[ind]+0.05, mL1[ind], horizontalalignment='center', color='w', weight='semibold')
    #cma_stats.text(tick+.2, m1[ind]+1, mL1[ind], horizontalalignment='center', color='w', weight='semibold')
    #ind += 2
sns.despine(left = True, bottom = True)
plt.show()
plt.savefig("output/walkingstats-random.png")