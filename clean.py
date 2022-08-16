from pyspark.sql import SparkSession
from pyspark.pandas.config import set_option, reset_option
import pyspark as ps
import pandas as pd
import glob
import sys

set_option("compute.ops_on_diff_frames", True)
spark = SparkSession.builder.getOrCreate()
# ps = DataFrame.
c = sys.argv[1]


dbs = ps.pandas.read_csv("data/idx_dbs_coords_CMA.csv")[['dbuid', 'idx', 'x', 'y', 
                                                     'lon', 'lat', 'CMANAME', 'PRUID']]
    
# read filtered ttm data
temp = []
for file in glob.glob(f"output/ttms-filtered-{c}-*.csv"):
    print(file)
    temp.append(ps.pandas.read_csv(file))

print('concatenating...')
cma_df = ps.pandas.concat(temp)


# double merge to get dbuid of src and dst
print("merge 1...")
merge1 = dbs.merge(cma_df, how = "right",
         left_on = "idx",
         right_on = "idx_src")

print("merge 2...")
merge2 = dbs[['idx','dbuid']].merge(merge1, how = "right",
                           left_on = "idx",
                           right_on = "idx_dst",
                           suffixes = ["_dest", "_source"]
                          )

# export
print("converting to minutes...")
dur = merge2.duration/60

print("exporting...")
dur.to_pandas().to_csv(f"output/summary/{c}-durations.csv", index = False) 