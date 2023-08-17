#!/bin/bash

#SBATCH --output="logs/slurm-%x-%A.out"
#SBATCH --job-name=test
#SBATCH --nodes=1
#SBATCH --cpus-per-task=8
#SBATCH --partition=normal
#SBATCH --exclude=cn10,cn8,cn24,cn25
#SBATCH --no-kill

#### JOB LOGIC ###
export OMP_NUM_THREADS=1
export USE_SIMPLE_THREADED_LEVEL3=1
export MKL_NUM_THREADS=1

source activate FINE_Nestor_new 

#SCENARIO_NAME="newTHG0"
SCENARIO_NAME="_test_Minisystem"

LOG_FILE_NAME="logs/slurm-"$SLURM_JOB_NAME"-"$SLURM_JOB_ID".out"
#python test/test_miniSystem.py
python scripts_nestor/start_cluster.py $SCENARIO_NAME $LOG_FILE_NAME 
