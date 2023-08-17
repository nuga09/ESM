from nestor.nestor import Nestor
import os
import json
import numpy as np


def test_minisystem():
    # 1. get json and compare
    # 1.1 load json
    json_path = os.path.abspath(os.path.join(
        os.path.dirname(__file__),
        "..", "nestor", "data", "scenario_definition", "_test_Minisystem.json"))
    with open(json_path) as f:
        test_json = json.load(f)

    # 1.2 correct json
    correct_json = {
        "Run": "Mini-System",
        "datalocation": "local",
        "databasename": "Energysystemdaten_Minisystem_V2_SpeicherFix.xlsx",
        "inputprofiledataname": "dummy.csv",
        "outputprofiledataname": "dummy.csv",
        "historicaldataname": "Historical_data_enerG_minisystem.xlsx",
        "forceddecommissioningdataname": "ForcedDecomissioning_minisystem.xlsx",
        "heatloaddataname": "Heat_Load_profile_GER_2013.csv",
        "evaluationfilename": "evaluation_file_V34_FINE.xlsx",
        "optparam_json": "default",
        "GHGgoals_json": "GHG_Minisystem",
        "transformation_path": "backcasting",
        "exportFlowResults": True,
        "maxRefurbRate": 2.5,
        "QP": False,
        "solver": "glpk",
        "typdays": 48,
        "startyear": 2025,
        "targetyear": 2045,
        "referenceyear": 2020,
        "interval": 5,
        "WACC": 0.1,
        "considerMaxYearlyExpansionLimit": False,
        "maxYearlyExpansionGW_Onshore": 8,
        "maxYearlyExpansionGW_Offshore": 4,
        "maxYearlyExpansionGW_OFPV": 10,
        "maxYearlyExpansionGW_RTPV": 10,
        "sCurveParam": [
            1.1,
            0.22,
            129.2961
        ],
        "UsableBEVStorageCapacity_per_km": 3.04136253,
        "string_identifiers": {
            "controlunit": "-control",
            "TSource": "-TSource-",
            "TSink": "-TSink-",
            "geothermalHeatPumps": "GeothermHeatPump",
            "CO2Sinks": "-Snk-NonCO2Emission",
            "onshore": "P-Trans-WindOnshore",
            "offshore": "P-Trans-WindOffshore",
            "rooftop_pv": "P-Trans-SolarPVRT",
            "openfield_pv": "P-Trans-SolarPVOF",
            "AEDemand": "AEDem-el",
            "LightingN-AEDemand": "S-Dem-LightingNAEDem-el",
            "IndustryDemand": "I-Dem-",
            "BatteryElectricCar": "T-Trans-PCarBEV",
            "BatteryElectricCar_Storage": "P-Stor-BEVintStorage-el",
            "Sanierungspaket1": "SP1",
            "building_exclusion_list": [
                "_stock",
                "4",
                "-K55",
                "NWGWG-N",
                "NWG1-N",
                "NWG2-N",
                "NWG3-N",
                "-HCFINE-HeatingHub",
                "_newHub",
                "-control",
                "-TSource-",
                "-Hub-",
                "-Dem-",
                "-TSink-"
            ],
            "building_standards": [
                "-K",
                "-NE",
                "SP"
            ],
            "building_filter_for_helper_components": "-HCFINE-",
            "refurbishmentpackages": [
                "-SP1",
                "-SP2"
            ],
            "building_categories": [
                "EZFH",
                "MFH",
                "NWG"
            ]
        }
    }

    differences = []
    requirements = {}
    for param in correct_json.keys():
        if param == "solver":
            continue
        if correct_json[param] != test_json[param]:
            differences.append(param)
            requirements[param] = correct_json[param]
    if len(differences) > 0:
        print(f"The test json has incorrect input for '{differences}'")
        print(f"Please update to: {requirements}")
        raise ValueError("See log above and adjust _test_Minisystem.json")

    # 2. run and compare
    test = Nestor("_test_Minisystem")
    if test_json["solver"] == "gurobi":
        np.testing.assert_almost_equal(test.esM.pyM.Obj(), 111314020.61490926)
    elif test_json["solver"] == "glpk":
        np.testing.assert_almost_equal(test.esM.pyM.Obj(), 111314020.48004568)


if __name__ == "__main__":
    test_minisystem()
