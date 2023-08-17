#!/bin/bash

#SBATCH --output="/storage/internal/data/ra-maier/exchangeFelix/logs/slurm-%x-%A.out"
#SBATCH --job-name=enercoretest
#SBATCH --nodes=1
#SBATCH --cpus-per-task=1
#SBATCH --partition=normal
#SBATCH --no-kill
#SBATCH --exclude=cn10



#### JOB LOGIC ###
export OMP_NUM_THREADS=1
export USE_SIMPLE_THREADED_LEVEL3=1
export MKL_NUM_THREADS=1

source activate envNestor

#SCENARIO_NAME="_study_THG0_demehub"
# SCENARIO_NAME="FINEtest"
SCENARIO_NAME="newTHG0"
# SCENARIO_NAME="_test_Minisystem"

LOG_FILE_NAME="logs/slurm-"$SLURM_JOB_NAME"-"$SLURM_JOB_ID".out"
python nestor/workflow.py $SCENARIO_NAME $LOG_FILE_NAME 
