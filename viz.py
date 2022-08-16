import pandas as pd
import glob
import re
import seaborn as sns
import matplotlib.pyplot as plt

# Read duration files
files = []
for file in glob.glob("output/summary/*"):

    temp = pd.read_csv(file)
    
    fname = re.sub("output/summary/", "", file)
    fname = re.sub(".csv", "", fname)
    print(fname)
    
    temp['Prov/Terr'] = fname
    files.append(temp)
    
ttm = pd.concat(files)
ttm = ttm.pivot(
    values = "duration",
    columns = "Prov/Terr"
)


# Seaborn Boxplot ----
plt.figure(figsize=(10,8))
sns.set_style("white", {'axes.grid':False})

cma_stats = sns.boxplot(data = ttm, showmeans = True, meanprops = {"marker":"o",
                                                               "markerfacecolor":"white",
                                                               "markeredgecolor":"black",
                                                               "markersize":"8"}, palette="Blues")

print("Plotting...")
medians = ttm.median().apply(lambda x: round(x, 1)).values
medians = pd.Series(medians)
vertical_offset = ttm.median().median() * 0.1
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
plt.savefig("output/cyclingstats.png")