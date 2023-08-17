import pandas as pd
import numpy as np
import openpyxl
import os


class PotentialUpdate():
    def __init__(self, renewable_definition, nestor_ee_path, parameter_file_path):
        self.renewable_definition = renewable_definition
        self.nestor_ee_path = nestor_ee_path
        self.parameter_file_path = parameter_file_path

        self.renewable_technologies = []
        self.case_paths = {}
        for renewable_tech, renewable_info in self.renewable_definition.items():
            if renewable_tech not in ["onshore", "rooftop_pv", "offshore", "openfield_pv"]:
                raise NotImplementedError(
                    "Check naming of {} in scenario_definition-json".format(renewable_tech) +
                    "Name should be either 'onshore','rooftop_pv','offshore','openfield_pv'")

            self.renewable_technologies.append(renewable_tech)
            self.case_paths[renewable_tech] = os.path.join(
                self.nestor_ee_path,
                renewable_tech,
                renewable_info["nestor_ee_case"])

        # A. Parameterfile
        # 1. TODO check for renewables in Parameter-File!
        self.precheck_parameter_file()

        # 2. Prepare data
        self.prepare_data()

    def precheck_parameter_file(self):

        # set up string lists to check the parameter file for
        check_strings = {"onshore": ["Onshore", "onshore"],
                         "rooftop_pv": ["rtpv", "RTPV"],
                         "openfield_pv": ["opfv", "OFPV"],
                         "offshore": ["Offshore", "offshore"]}
        not_updated = [x for x in check_strings.keys(
        ) if x not in self.renewable_technologies]
        error_keys = []
        for i in self.renewable_technologies:
            error_keys.extend(check_strings[i])

        ee_keys = []
        for i in not_updated:
            ee_keys.extend(check_strings[i])

        # Sheets
        for sheet_name in ["Sources", "Hubs", "Transformers"]:
            components = pd.read_excel(
                self.parameter_file_path, sheet_name=sheet_name)["name"]
            ee_components_warning = [x for x in components if any(
                [y for y in ee_keys if (y in x) and ("CO2" not in x)])]

            ee_components_error = [x for x in components if any(
                [y for y in error_keys if (y in x) and ("CO2" not in x)])]

            if len(ee_components_warning) > 0:
                print(
                    "Warning: Potential for "
                    + "'{}' is not coupled ".format(not_updated)
                    + "but old data in parameter-file is used.'")
            if len(ee_components_error) > 0:
                # raise ValueError(
                print(
                    "Error renewable coupling: Components " +
                    "'{}' ".format(ee_components_error) +
                    "are in pre-coupled raw parameter-file in " +
                    "sheet {}".format(sheet_name))
        # Sheet Connectors
        connections = pd.read_excel(
            self.parameter_file_path, sheet_name="Connectors")
        input_ee_warning = [x for x in connections["input"] if any(
            [y for y in ee_keys if (y in x) and ("CO2" not in x)])]
        output_ee_warning = [x for x in connections["output"] if any(
            [y for y in ee_keys if (y in x) and ("CO2" not in x)])]

        input_ee_error = [x for x in connections["input"] if any(
            [y for y in error_keys if (y in x) and ("CO2" not in x)])]
        output_ee_error = [x for x in connections["output"] if any(
            [y for y in error_keys if (y in x) and ("CO2" not in x)])]

        if len(input_ee_warning) > 0 or len(output_ee_warning) > 0:
            print("Warning: Potential for "
                  + "'{}' is not coupled ".format(not_updated)
                    + "but old data in Connectors-file is used.'")

        if len(input_ee_error) > 0 and len(output_ee_error) > 0:
            # raise ValueError(
            print(
                "Error renewable coupling: Components " +
                "'{}' and ''{} ".format(input_ee_error, output_ee_error) +
                "are in pre-coupled raw parameter-file in " +
                "sheet {}".format(sheet_name))

    def prepare_data(self):
        self.res_Sources = {}
        self.res_Sinks = {}
        self.res_Hubs = {}
        self.res_Connectors = {}
        self.res_Transformers = {}
        self.connections_helper = {}
        self.groups = {}
        self.timeseries = {}
        self.historical_data = {}
        self.stock_grid_connection = {}
        self.TSources = {}
        self.raw_group_name_mapping = {}
        self.base_names = {}

        # get cols for each sheet
        self.sources_cols = list(pd.read_excel(
            self.parameter_file_path, sheet_name="Sources").columns)
        self.hubs_cols = list(pd.read_excel(
            self.parameter_file_path,
            sheet_name="Hubs").columns)
        self.connector_cols = list(pd.read_excel(
            self.parameter_file_path, sheet_name="Connectors").columns)
        self.transformer_cols = list(pd.read_excel(
            self.parameter_file_path, sheet_name="Transformers").columns)
        self.sinks_cols = list(pd.read_excel(
            self.parameter_file_path, sheet_name="Sinks").columns)

        for renewable_tech, renewable_info in self.renewable_definition.items():
            # get all groups
            potential_path = os.path.join(
                self.case_paths[renewable_tech], "potentials.csv")
            _db_potentials = pd.read_csv(potential_path, index_col=0)

            self.groups[renewable_tech] = list(_db_potentials.index)
            not_stock_groups = [
                x for x in list(_db_potentials.index) if "_stock" not in x]

            # get base name of potential and stock components (is required, as region can have stock but no potential)
            base_of_stock_groups = [
                x.replace("_stock", "") for x in list(_db_potentials.index) if "_stock" in x]
            self.base_names[renewable_tech] =\
                sorted(
                    list(set(not_stock_groups+base_of_stock_groups)))

            self.connections_helper[renewable_tech] = \
                pd.DataFrame(
                columns=["Tech", "hasPotential", "TechStock", "hasStock",
                         "Hubs",
                         "TSource",
                         "GridConnection",
                         "GridConnectionStock",
                         "ElectrolyserGridConnection"],
                index=self.base_names[renewable_tech])
            self.connections_helper[renewable_tech]["Tech"] =\
                [naming(group=x, category="Source", renewable_tech=renewable_tech)
                 for x in self.connections_helper[renewable_tech].index]
            self.connections_helper[renewable_tech]["TechStock"] = \
                [naming(group=x, category="Source", renewable_tech=renewable_tech) +
                 "_stock" for x in self.connections_helper[renewable_tech].index]

            for idx, row in self.connections_helper[renewable_tech].iterrows():

                self.connections_helper[renewable_tech].loc[idx, "hasPotential"] =\
                    idx in list(_db_potentials.index)
                self.connections_helper[renewable_tech].loc[idx, "hasExisting"] =\
                    (idx+"_stock") in list(_db_potentials.index)

            # prepare parameter file
            self.prepare_Sources(
                renewable_tech, renewable_info, _db_potentials)
            self.prepare_Sinks(renewable_tech)
            self.prepare_Hubs(renewable_tech)
            self.prepare_Transformers(renewable_tech)
            self.prepare_Connectors(renewable_tech)

            # prepare timeseries and historical data files
            self.prepare_timeseries(renewable_tech)
            self.prepare_historical_data(renewable_tech)

    def prepare_timeseries(self, renewable_tech):
        ts_path = os.path.join(
            self.case_paths[renewable_tech], "timeseries.csv")
        self.timeseries[renewable_tech] = pd.read_csv(ts_path, index_col=0)

    def prepare_historical_data(self, renewable_tech):
        historical_path = os.path.join(
            self.case_paths[renewable_tech], "historical.csv")
        self.historical_data[renewable_tech] = pd.read_csv(
            historical_path, index_col=0)
        self.historical_data[renewable_tech].columns = [naming(
            group=x, category="Source", renewable_tech=renewable_tech)for x in self.historical_data[renewable_tech].columns]

        self.historical_data[renewable_tech].index = [int(x) if x.isdigit(
        ) else x for x in self.historical_data[renewable_tech].index]

        # TODO implement life time to determine historical stock of grid
        # currently just use lifetime of renewable technology
        for grid in self.stock_grid_connection[renewable_tech]:
            raw_group = get_group_of_name(
                name=grid, category="Transformer-Grid",
                renewable_tech=renewable_tech)
            source_name = naming(
                group=raw_group, category="Source",
                renewable_tech=renewable_tech)
            self.historical_data[renewable_tech][grid.replace("_stock", "")] = \
                self.historical_data[renewable_tech][source_name.replace(
                    "_stock", "")]

    def prepare_Sources(self, renewable_tech, renewable_info, _db_potentials):
        # prepare Sources
        # 1. Prepare Potentials
        _potentials = pd.DataFrame(columns=self.sources_cols)

        # information from db
        _potentials[["lb", "ub", "profile"]] = \
            _db_potentials[["lb", "ub", "profile"]]
        _potentials["name"] = [
            naming(group=x, category="Source", renewable_tech=renewable_tech)
            for x in _potentials.index]
        # hard coded
        _potentials["CO2footprint"] = 0
        _potentials["controllable"] = True
        _potentials["primaryenergy"] = 1
        _potentials["finalenergy"] = 0
        _potentials["renewable"] = True
        _potentials["energytype"] = "power"
        _potentials["intComp"] = False
        _potentials["size"] = "None"
        _potentials["unrestrict"] = False
        # add pos x and y hard coded
        _potentials["pos_x"] = 100
        if renewable_tech == "onshore":
            _potentials["pos_y"] = 5200
        elif renewable_tech == "rooftop_pv":
            _potentials["pos_y"] = 6200
        elif renewable_tech == "openfield_pv":
            _potentials["pos_y"] = 5800
        elif renewable_tech == "offshore":
            _potentials["pos_y"] = 4800
        else:
            raise NotImplementedError()

        # parameter in json:
        json_param = ["capex", "capex20", "capex30", "capex40",
                      "capex50", "opex_fix", "opex_var", "lifetime",
                      "tech_lifetime", "cost_scale2020", "cost_scale2050"]
        for param in json_param:
            if "capex" in param:
                for component, component_data in _potentials.iterrows():
                    if "_stock" in component_data["name"]:
                        _potentials.loc[component,
                                        param] = renewable_info["capex20"]
                    else:
                        _potentials.loc[component,
                                        param] = renewable_info[param]
            elif "cost_scale" in param:
                for component, component_data in _potentials.iterrows():
                    if "_stock" in component_data["name"]:
                        _potentials.loc[component, param] = 0
                    else:
                        _potentials.loc[component,
                                        param] = renewable_info[param]
            else:
                _potentials[param] = renewable_info[param]

        if renewable_info["WACC"] is None:
            _potentials["WACC"] = "None"
        else:
            _potentials["WACC"] = renewable_info["WACC"]

        # 2. Prepare TSources for Hubs
        _TSources = pd.DataFrame(columns=self.sources_cols)
        # add TSource to "normal"/not stock components to connections
        self.connections_helper[renewable_tech]["TSource"] =\
            [naming(group=x, category="TSource", renewable_tech=renewable_tech)
             for x in self.connections_helper[renewable_tech].index]
        # add TSources to sources
        self.TSources[renewable_tech] = [naming(group=x, category="TSource", renewable_tech=renewable_tech)
                                         for x in self.base_names[renewable_tech]]
        _TSources["name"] = self.TSources[renewable_tech]
        _TSources["profile"] = "None"
        _TSources["CO2footprint"] = 0
        _TSources["controllable"] = True
        _TSources["primaryenergy"] = 1
        _TSources["finalenergy"] = 0
        _TSources["renewable"] = False
        for i in ["capex", "capex20", "capex30", "capex40",
                  "capex50", "opex_var", ]:
            _TSources[i] = 10000
        for j in ["lifetime",
                  "tech_lifetime"]:
            _TSources[j] = 20
        _TSources["opex_fix"] = 0
        _TSources["energytype"] = "power"
        _TSources["lb"] = 0
        _TSources["ub"] = 500
        _TSources["pos_x"] = 0
        _TSources["pos_y"] = 0
        _TSources["WACC"] = "None"
        _TSources["intComp"] = False
        _TSources["size"] = "None"
        _TSources["cost_scale2020"] = 0
        _TSources["cost_scale2050"] = 0
        _TSources["unrestrict"] = True
        # add to dict
        self.res_Sources[renewable_tech] = pd.concat([_potentials, _TSources])

    def prepare_Sinks(self, renewable_tech):
        _TSink = pd.DataFrame(columns=self.sinks_cols)
        _TSink["name"] = [naming(group=x, category="TSink", renewable_tech=renewable_tech)
                          for x in self.base_names[renewable_tech]]
        _TSink["profile"] = "None"
        _TSink["CO2footprint"] = 0
        _TSink["controllable"] = True
        _TSink["primaryenergy"] = 0
        _TSink["finalenergy"] = 0
        _TSink["energytype"] = "power"
        _TSink["lb"] = 0
        _TSink["ub"] = 500
        _TSink["pos_x"] = 0
        _TSink["pos_y"] = 0
        _TSink["WACC"] = "None"
        _TSink["intComp"] = False
        _TSink["size"] = "None"
        _TSink["cost_scale2020"] = 0
        _TSink["unrestrict"] = True
        _TSink["renewable"] = False
        _TSink["capex"] = 5000
        _TSink["opex_fix"] = 0
        _TSink["opex_var"] = 100
        _TSink["lifetime"] = 20

        self.connections_helper[renewable_tech]["TSink"] =\
            [naming(group=x, category="TSink", renewable_tech=renewable_tech)
             for x in self.connections_helper[renewable_tech].index]

        # add to dict
        self.res_Sinks[renewable_tech] = _TSink

    def prepare_Hubs(self, renewable_tech):
        _hubs = pd.DataFrame(columns=self.hubs_cols)
        # naming convention ?
        _hubs["name"] = [naming(group=x, category="Hub", renewable_tech=renewable_tech)
                         for x in self.base_names[renewable_tech]]
        self.connections_helper[renewable_tech]["Hubs"] =\
            [naming(group=x, category="Hub", renewable_tech=renewable_tech).replace(
                "_stock", "") for x in self.connections_helper[renewable_tech].index]
        _hubs["energytype"] = "power"
        _hubs["capex"] = 0
        _hubs["opex_fix"] = 0
        _hubs["opex_var"] = 0
        _hubs["lifetime"] = self.renewable_definition[renewable_tech]["lifetime"]
        _hubs["lb"] = 0
        _hubs["ub"] = 500
        _hubs["WACC"] = "None"
        _hubs["intComp"] = False
        _hubs["size"] = "None"
        _hubs["cost_scale"] = 0
        _hubs["pos_x"] = 1500
        _hubs["pos_y"] = 8000
        _hubs["unrestrict"] = True
        # add to dict
        self.res_Hubs[renewable_tech] = _hubs

    def prepare_Transformers(self, renewable_tech):
        # grids
        _normal_grid_connection = [
            naming(group=x, category="Transformer-Grid", renewable_tech=renewable_tech) for x in self.base_names[renewable_tech]]
        # TODO existing grid connection
        self.stock_grid_connection[renewable_tech] = [
            naming(group=x, category="Transformer-Grid", renewable_tech=renewable_tech)+"_stock" for x in self.base_names[renewable_tech]]
        _grid_connections = _normal_grid_connection + \
            self.stock_grid_connection[renewable_tech]
        _transformers_grid_connections = pd.DataFrame(
            columns=self.transformer_cols, index=_grid_connections)
        self.connections_helper[renewable_tech].loc[
            self.base_names[renewable_tech], "GridConnection"] = \
            _normal_grid_connection
        self.connections_helper[renewable_tech].loc[
            self.base_names[renewable_tech], "GridConnectionStock"] = \
            self.stock_grid_connection[renewable_tech]

        # Add hard coded Parameter
        _transformers_grid_connections["efficiency"] = 1
        _transformers_grid_connections["dimEnergyType"] = "power"
        _transformers_grid_connections["flh_lb"] = 0
        _transformers_grid_connections["flh_ub"] = "None"
        _transformers_grid_connections["CO2footprint2020"] = 0
        _transformers_grid_connections["CO2footprint2050"] = 0
        _transformers_grid_connections["lb"] = 0
        _transformers_grid_connections["energytype"] = "power"
        _transformers_grid_connections["intComp"] = False
        _transformers_grid_connections["size"] = "None"
        _transformers_grid_connections["cost_scale2020"] = 0
        _transformers_grid_connections["cost_scale2050"] = 0
        _transformers_grid_connections["mfaMean"] = "None"
        _transformers_grid_connections["mfaSd"] = "None"
        _transformers_grid_connections["pos_x"] = 1600
        _transformers_grid_connections["pos_y"] = 8000
        _transformers_grid_connections["unrestrict"] = False

        # TODO zweimal opex_fix und opex var
        # TODO nur einmal capex nehmen
        grid_connection_param = self.renewable_definition[renewable_tech]["grid_connection"]
        for _gc in _transformers_grid_connections.index:
            for i in ["capex", "capex20", "capex30", "capex40", "capex50", "opex_fix", "opex_var"]:
                if "_stock" in _gc:
                    _transformers_grid_connections.loc[_gc, i] = 0
                else:
                    _transformers_grid_connections.loc[_gc, i] = \
                        grid_connection_param[i]
            if "_stock" in _gc:
                _transformers_grid_connections.loc[_gc, "ub"] = 0
            else:
                _transformers_grid_connections.loc[_gc, "ub"] = 500
        for i in ["opex_fix", "opex_var", "lifetime", "tech_lifetime"]:
            _transformers_grid_connections[i] = grid_connection_param[i]

        if grid_connection_param["WACC"] is None:
            _transformers_grid_connections["WACC"] = "None"
        else:
            _transformers_grid_connections["WACC"] = grid_connection_param["WACC"]

        _transformers_grid_connections["name"] = _transformers_grid_connections.index

        _transformers_electrolyzers = pd.DataFrame(
            columns=self.transformer_cols)
        # electrolysers?
        if self.renewable_definition[renewable_tech]["electrolyser_connection"]:
            names = [naming(group=x, category="Transformer-Electrolyzer", renewable_tech=renewable_tech)
                     for x in self.base_names[renewable_tech]]
            _transformers_electrolyzers = pd.DataFrame(
                columns=self.transformer_cols,
                index=names)
            self.connections_helper[renewable_tech].loc[
                self.base_names[renewable_tech], "ElectrolyserGridConnection"] = \
                _transformers_electrolyzers.index
            _transformers_electrolyzers["efficiency"] = 1
            _transformers_electrolyzers["dimEnergyType"] = "h2power"
            _transformers_electrolyzers["CO2footprint2020"] = 0
            _transformers_electrolyzers["CO2footprint2050"] = 0
            _transformers_electrolyzers["flh_lb"] = 0
            _transformers_electrolyzers["flh_ub"] = "None"
            _transformers_electrolyzers["lb"] = 0
            _transformers_electrolyzers["ub"] = 500
            _transformers_electrolyzers["energytype"] = "h2power"
            _transformers_electrolyzers["pos_x"] = 1600
            _transformers_electrolyzers["pos_y"] = 8000
            _transformers_electrolyzers["WACC"] = "None"
            _transformers_electrolyzers["intComp"] = False
            _transformers_electrolyzers["size"] = "None"
            _transformers_electrolyzers["cost_scale2020"] = 0
            _transformers_electrolyzers["cost_scale2050"] = 0
            _transformers_electrolyzers["mfaMean"] = "None"
            _transformers_electrolyzers["mfaSd"] = "None"
            _transformers_electrolyzers["unrestrict"] = False
            # add parameter from json
            electroylzer_connection_param = self.renewable_definition[
                renewable_tech]["electrolyser_connection_param"]
            for i in ["capex", "capex20", "capex30", "capex40", "capex50"]:
                _transformers_electrolyzers[i] = electroylzer_connection_param["capex"]

            for j in ["opex_fix", "opex_var", "lifetime", "tech_lifetime"]:
                _transformers_electrolyzers[j] = electroylzer_connection_param[j]

            _transformers_electrolyzers["name"] = _transformers_electrolyzers.index

        # add to dict
        _transformers = _transformers_grid_connections.append(
            _transformers_electrolyzers)
        self.res_Transformers[renewable_tech] = _transformers

    def prepare_Connectors(self, renewable_tech):
        _connections = pd.DataFrame(columns=self.connector_cols)
        for group, connections in self.connections_helper[renewable_tech].iterrows():
            # from tech to hub
            if connections["hasPotential"]:
                _connections = _connections.append(
                    {"input": naming(group=group, category="Source", renewable_tech=renewable_tech),
                     "output": connections["Hubs"],
                     "efficiency": 1,
                     "efficiency20": 1,
                     "efficiency30": 1,
                     "efficiency40": 1,
                     "efficiency50": 1}, ignore_index=True)

            # from TSource to hub
            _TSource_connections = connections.dropna()
            _connections = _connections.append(
                {"input": _TSource_connections["TSource"],
                 "output": _TSource_connections["Hubs"],
                 "efficiency": 1,
                 "efficiency20": 1,
                 "efficiency30": 1,
                 "efficiency40": 1,
                 "efficiency50": 1}, ignore_index=True)

            # from stock tech to hub
            if connections["hasExisting"]:
                _connections = _connections.append(
                    {"input": connections["TechStock"],
                     "output": connections["Hubs"],
                     "efficiency": 1,
                     "efficiency20": 1,
                     "efficiency30": 1,
                     "efficiency40": 1,
                     "efficiency50": 1}, ignore_index=True)

            # from hub to gridconnection
            _connections = _connections.append(
                {"input": connections["Hubs"],
                    "output": connections["GridConnection"],
                    "efficiency": 1,
                    "efficiency20": 1,
                    "efficiency30": 1,
                    "efficiency40": 1,
                    "efficiency50": 1}, ignore_index=True)

            # from hub to gridconnection_stock
            _connections = _connections.append(
                {"input": connections["Hubs"],
                    "output": connections["TSink"],
                    "efficiency": 1,
                    "efficiency20": 1,
                    "efficiency30": 1,
                    "efficiency40": 1,
                    "efficiency50": 1}, ignore_index=True)

            # from hub to TSink
            _connections = _connections.append(
                {"input": connections["Hubs"],
                    "output": connections["GridConnectionStock"],
                    "efficiency": 1,
                    "efficiency20": 1,
                    "efficiency30": 1,
                    "efficiency40": 1,
                    "efficiency50": 1}, ignore_index=True)

            # grid to central grids
            grid_params = self.renewable_definition[renewable_tech]["grid_connection"]
            for grid_connection in ["GridConnection", "GridConnectionStock"]:
                _connections = _connections.append(
                    {"input": connections[grid_connection],
                        "output": "P-Hub-DistributionGridElectricityHub-el",
                        "efficiency": grid_params["distribution_grid_efficiency"],
                        "efficiency20": grid_params["distribution_grid_efficiency"],
                        "efficiency30": grid_params["distribution_grid_efficiency"],
                        "efficiency40": grid_params["distribution_grid_efficiency"],
                        "efficiency50": grid_params["distribution_grid_efficiency"]},
                    ignore_index=True)
                _connections = _connections.append(
                    {"input": connections[grid_connection],
                        "output": "P-Hub-TransmissionGridElectricityHub-el",
                        "efficiency": grid_params["transmission_grid_efficiency"],
                        "efficiency20": grid_params["transmission_grid_efficiency"],
                        "efficiency30": grid_params["transmission_grid_efficiency"],
                        "efficiency40": grid_params["transmission_grid_efficiency"],
                        "efficiency50": grid_params["transmission_grid_efficiency"]},
                    ignore_index=True)
            # electrolysers?
            if self.renewable_definition[renewable_tech]["electrolyser_connection"]:
                # from hub for electrolyzer grid
                _connections = _connections.append(
                    {"input": connections["Hubs"],
                        "output": connections["ElectrolyserGridConnection"],
                        "efficiency": 1,
                        "efficiency20": 1,
                        "efficiency30": 1,
                        "efficiency40": 1,
                        "efficiency50": 1}, ignore_index=True)
                # from electrolyzer grid to "Electrolyser-Ehub"
                _connections = _connections.append(
                    {"input": connections["ElectrolyserGridConnection"],
                        "output": "P-Hub-ElectrolyserEHub-el",
                        "efficiency": 1,
                        "efficiency20": 1,
                        "efficiency30": 1,
                        "efficiency40": 1,
                        "efficiency50": 1}, ignore_index=True)
        self.res_Connectors[renewable_tech] = _connections

    def update_parameter_files(self, targetpath):
        book = openpyxl.load_workbook(self.parameter_file_path)
        for renewable_tech in self.renewable_definition.keys():
            append_rows(self.res_Sources[renewable_tech], book["Sources"])
            append_rows(self.res_Hubs[renewable_tech], book["Hubs"])
            append_rows(
                self.res_Connectors[renewable_tech], book["Connectors"])
            append_rows(
                self.res_Transformers[renewable_tech], book["Transformers"])
            append_rows(self.res_Sinks[renewable_tech], book["Sinks"])

        name = os.path.basename(self.parameter_file_path).split(".xlsx")[
            0]+"_processed.xlsx"
        path = os.path.join(targetpath, name)
        book.save(path)
        return path

    def update_timeseries(self, original_path, targetpath):
        data = pd.read_csv(original_path, index_col=0)
        for tech in self.renewable_technologies:
            data[self.timeseries[tech].columns] = self.timeseries[tech]
        name = os.path.basename(original_path).split(".csv")[
            0]+"_processed.csv"
        path = os.path.join(targetpath, name)
        data.to_csv(path)
        return path

    def update_historical_data(self, original_path, targetpath):
        data = pd.read_excel(original_path, index_col=0)
        for tech in self.renewable_technologies:
            data[self.historical_data[tech].columns] = self.historical_data[tech]
        name = os.path.basename(original_path).split(".xlsx")[
            0]+"_processed.xlsx"
        data_path = os.path.join(targetpath, name)
        data.to_excel(data_path)
        return data_path


#############################################################
# UTILS


def naming(group: str, category: str, renewable_tech: str):
    if renewable_tech == "onshore":
        tech_string = "WindOnshore"
    elif renewable_tech == "offshore":
        tech_string = "WindOffshore"
    elif renewable_tech == "rooftop_pv":
        tech_string = "SolarPVRT"
    elif renewable_tech == "openfield_pv":
        tech_string = "SolarPVOF"
    else:
        raise NotImplementedError()

    # create names based on category
    if category == "Source":
        group_str = "P-Trans-{}{}-el".format(tech_string, group)
    elif category == "TSource":
        group_str = "P-TSource-{}{}Hub-el".format(tech_string, group)
    elif category == "TSink":
        group_str = "P-TSink-{}{}Hub-el".format(tech_string, group)
    elif category == "Transformer-Grid":
        group_str = "P-Grid-{}{}-el".format(tech_string, group)
    elif category == "Transformer-Electrolyzer":
        group_str = "P-Grid-{}{}Electrolyzer-el".format(tech_string, group)
    elif category == "Hub":
        group_str = "P-Hub-{}{}Hub-el".format(tech_string, group)
    else:
        raise NotImplementedError(
            "Category {} not implemented for renaming".format(category))

    # add stock if it is a stock component
    if "_stock" in group:
        group_str = group_str.replace("_stock", "")+"_stock"
    return group_str


def get_group_of_name(name: str, category: str, renewable_tech: str):
    _id = "   "
    _helper_string = naming(group=_id,
                            category=category, renewable_tech=renewable_tech)
    string_parts = _helper_string.split(_id)
    group = name
    for _str_part in string_parts:
        group = group.replace(_str_part, "")
    return group


def append_rows(df: pd.DataFrame, sheet):
    """Add rows of df to sheet of Nestor-param file.

    Parameters
    ----------
    df : pd.DataFrame
    sheet : openpyxl.worksheet.worksheet.Worksheet
        sheet in Nestor parameter .xlsx data to add rows to
    """
    for index, row in df.iterrows():
        # convert bool-value in list to string
        append_row_values = [str(x) if type(x) == bool or type(
            x) == np.bool_ else x for x in row]
        sheet.append(append_row_values)
