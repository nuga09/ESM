#!/bin/bash

#SBATCH --output="logs/slurm-%x-%A.out"
#SBATCH --job-name=nestor
#SBATCH --nodes=1
#SBATCH --cpus-per-task=8
#SBATCH --nice=5
#SBATCH --partition=normal
#SBATCH --exclude=cn10,cn8
#SBATCH --no-kill

#### JOB LOGIC ###
export OMP_NUM_THREADS=1
export USE_SIMPLE_THREADED_LEVEL3=1
export MKL_NUM_THREADS=1

source activate FINE_nestor

SCENARIO_NAME="newTHG0"
# SCENARIO_NAME="_test_Minisystem"

LOG_FILE_NAME="logs/slurm-"$SLURM_JOB_NAME"-"$SLURM_JOB_ID".out"
python nestor/workflow.py $SCENARIO_NAME $LOG_FILE_NAME 