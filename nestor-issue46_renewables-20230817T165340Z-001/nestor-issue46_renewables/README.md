<a href="https://www.fz-juelich.de/iek/iek-3/DE/Home/home_node.html"><img src="http://www.fz-juelich.de/SharedDocs/Bilder/IBG/IBG-3/DE/Plant-soil-atmosphere%20exchange%20processes/INPLAMINT%20(BONARES)/Bild3.jpg?__blob=poster" alt="Forschungszentrum Juelich Logo" width="230px"></a> 

# NESTOR

NESTOR (National Energy System Model with integrated Sector coupling) is an optimization model for the transformation of the Germany energy system. It  depicts the national energy supply – from primary energy to final energy – across all potential pathways and technologies. This integrated energy system model represents the sectors energy, industry, buildings and transport in detail through an hourly resolved network of energy sources, transformation processes, storages and energy demands. Its aim is to minimize the overall system costs. The most cost-efficient combination of technology and energy carrier is determined while taking into consideration externally determined framework conditions (e.g. greenhouse gas reduction targets) and assumptions (e.g. industrial goods production, transport demand). A distinctive feature of the model is that all possible reduction options compete with each other across all sectors (energy, transport, buildings, industry). 

NESTOR uses a myopic backcasting approach to evaluate the transformation of today's German energy system into a future energy system. Therefore, the target year is optimized first and a solution space for the transformation pathway is created. Next, today's existing energy system with it's  stocks is optimized as a first step of the system transformation. Then, each interval year from the start year to the target year is optimised,  while taking into account the stocks of the previous optimized years and the boundary conditions of the optimization space. As a result a cost-optimal transformation pathway, where every interval year is also cost-optimal, is obtained. 

Further key features include:
- Quadratic programming approach to take cost uncertainties into account
- Usage of time-series aggregation
- Detailed representation of industrial processes
- Options for Carbon Capture and Storage

A detailed explanation of the NESTOR model and its myopic backcasting approach is given in the following dissertation (in German):
Lopion, P. (2020), Modellgestützte Analyse kosteneffizienter CO2-Reduktionsstrategien. Publications of Forschungszentrum Jülich, “Energie & Umwelt” series, Vol. 506, (D82 Diss. RWTH Aachen, 2020) [Link](http://hdl.handle.net/2128/25562)

## Studies done with NESTOR 

Study | Year | Optimization Framework | enercore-branch | json | Nestor-Branch | Nestor-Version
--- | --- | --- | --- | --- | --- | ---
Dissertation Kullmann | 2021 | enercore | nestor | _dissertation_Kullmann | Master | dissertation_kullmann
KSG2045 | 2021 | enercore | nestor/THG0 | _study_THG0 | Master | KSG2045
Tag der Neugier 2022 | 2022 | enercore |nestor_advanced | several | project_TagDerNeugier2022 | ... branch

Check out to branch and tag of the study to find instructions for setup.
Note: ES2050 not versioned in this repository.

### Setup of NESTOR 

Nestor runs on the Framework of FINE from IEK-3 at FZJ.

#### Setup of FINE-NESTOR env
Clone the NESTOR repository.
```
cd <your path>
git clone git@jugit.fz-juelich.de:iek-3/groups/stationary-energy-systems/codenestor/nestor.git
git checkout develop
```

Clone and install [FINE](https://github.com/FZJ-IEK3-VSA/FINE) (dev branch)
```
cd <your path>
git clone git@jugit.fz-juelich.de:iek-3/shared-code/fine.git
cd FINE
``` 	

Clone and install Fine environment (FINE).
```
conda env create --name FINE_NESTOR --file requirements.yml
conda activate FINE_NESTOR
pip install -e .

```

If you want to use limitation of renewable energies expansion: 
```
git checkout Integrate_FINE_infrastructure

```


## Using NESTOR
### Problems of environment
- for problems with fiona check the version of geopandas (0.10.2 should work)
### Reproducing old studies
1. Checkout to tags listed in table above.
2. Run sbatch script in repository.

### Run new scenarios on the Cluster
1. Adapt configs to variate scenarios: 
    - scenario-json in Modell/Szenario-configs
    - related emmission restriction config in Modell/Szenario-configs/GHG_goals
    - related optimization parameter configs in Modell/Szenario-configs/Opt_parameter
2. Update name of scenario-json in sbatch script (FINE or enercore .sh script)
3. Run sbatch script

### Run new scenarios locally
1. as above
2. Update name in start.py 
3. Run start.py

### Analyse results
The scenario results can be found in the folder Modell/Results.
Here you find: 
- all input data
- all results data and evaluation file
- log file of the run


## License
Only internal usage at IEK3.

## About Us 
<a href="https://www.fz-juelich.de/iek/iek-3/DE/Home/home_node.html"><img src="https://www.fz-juelich.de/SharedDocs/Bilder/IEK/IEK-3/Abteilungen2015/VSA_DepartmentPicture_2019-02-04_459x244_2480x1317.jpg?__blob=normal" alt="Institut TSA"></a> 

We are the [Institute of Energy and Climate Research - Techno-economic Systems Analysis (IEK-3)](https://www.fz-juelich.de/iek/iek-3/DE/Home/home_node.html) belonging to the [Forschungszentrum Jülich](www.fz-juelich.de/). Our interdisciplinary institute's research is focusing on energy-related process and systems analyses. Data searches and system simulations are used to determine energy and mass balances, as well as to evaluate performance, emissions and costs of energy systems. The results are used for performing comparative assessment studies between the various systems. Our current priorities include the development of energy strategies, in accordance with the German Federal Government’s greenhouse gas reduction targets, by designing new infrastructures for sustainable and secure energy supply chains and by conducting cost analysis studies for integrating new technologies into future energy market frameworks.

## Contributions and Users


## Acknowledgement
