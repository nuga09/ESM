#!/bin/bash

#SBATCH --output="logs/slurm-%x-%A-%a.out"
#SBATCH --job-name=coupling
#SBATCH --nodes=1
#SBATCH --cpus-per-task=1
#SBATCH --array=0-16
#SBATCH --nice=5
#SBATCH --partition=normal
#SBATCH --exclude=cn10,cn8
#SBATCH --no-kill

#### JOB LOGIC ###
export OMP_NUM_THREADS=1
export USE_SIMPLE_THREADED_LEVEL3=1
export MKL_NUM_THREADS=1

source activate trep

TASK1=$((${SLURM_ARRAY_TASK_ID}+1))
turbine0=$((0))
turbine1=$((1))
turbine2=$((2))
turbine3=$((3))
turbine4=$((4))
turbine5=$((5))
turbine6=$((6))
turbine7=$((7))
turbine8=$((8))

python nestor/utils/create_renewable_db.py ${TASK1} ${turbine0} 
python nestor/utils/create_renewable_db.py ${TASK1} ${turbine1} 
python nestor/utils/create_renewable_db.py ${TASK1} ${turbine2} 
python nestor/utils/create_renewable_db.py ${TASK1} ${turbine3} 
python nestor/utils/create_renewable_db.py ${TASK1} ${turbine4} 
python nestor/utils/create_renewable_db.py ${TASK1} ${turbine5} 
python nestor/utils/create_renewable_db.py ${TASK1} ${turbine6} 
python nestor/utils/create_renewable_db.py ${TASK1} ${turbine7} 
python nestor/utils/create_renewable_db.py ${TASK1} ${turbine8} 
