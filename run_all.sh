#!/bin/bash
declare -a stringarray=(
                "Vancouver"
                # "Montréal"
                "Calgary"
                "Ottawa-Gatineau"
                "Toronto"
)

for value in ${stringarray[@]};
do
    echo $value
    
    # piecewise filter ttms by CMA in stringarray
    # python ttms.py $value
    
    # export duration summary
    python clean.py output/$value
    
    # save memory
    # rm -f output/$value-*.csv
    
done

python viz.py