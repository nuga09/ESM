import os
import json
import pandas as pd
import numpy as np
import FINE as fn

from nestor.utils.check_input import (check_input_data,
                                      check_scenario_definition,
                                      check_optimization_definition,
                                      check_renewable_input)
from nestor.utils.data_handling import get_raw_input_data_paths, folder_creation
from nestor.backcasting import MyopicTrans
from nestor.backcasting import _round

import warnings
warnings.filterwarnings('ignore', module='FINE')


class Nestor():
    def __init__(self, json_name):
        # Scenario and Run Definition
        self.data_processing_and_checking(json_name)

        # read in and process data
        self.read_data_parameterfile()
        self.read_timeseries()
        self.read_forced_decommissioning_and_historical_data()
        self.define_optimizaton_specs()
        self.get_component_parameters_over_transformationpathway()

        # 1. Backcasting
        if self.scenario_definition["transformation_path"] == "backcasting":
            self.currentyear = self.targetyear
            self.initalize_backcasting_network()
            self.backcasting = MyopicTrans(self)
            self.backcasting.transformation_pathway()
        # 2. Perfect Foresight
        else:
            raise ValueError("Implement FINE PERFECT FORESIGHT")

    def data_processing_and_checking(self, json_name):
        local_mainpath = os.path.join(os.path.dirname(__file__), "data")
        scenariopath = os.path.join(
            local_mainpath, "scenario_definition", "{}.json".format(json_name))
        with open(scenariopath) as f:
            self.scenario_definition = json.load(f)

        if self.scenario_definition["considerMaxYearlyExpansionLimit"] is True:
            raise ValueError(
                "Set 'considerMaxYearlyExpansionLimit' to False in the scenario definition json. Considering max expansion limit is yet to be implemented.")

        print("Json-Name: {}".format(json_name), flush=True)
        print(self.scenario_definition["Run"])
        check_scenario_definition(self.scenario_definition, local_mainpath)

        # Read and check Optimization-Variables from json
        optParapath = os.path.join(
            local_mainpath, "scenario_definition", "optimization_parameter",
            "{}.json".format(self.scenario_definition["optparam_json"]))
        with open(optParapath) as f:
            self.opt_parameters = json.load(f)
        check_optimization_definition(self.opt_parameters)

        self.get_first_parms()
        # Read GHG-targets from json
        GHGScenpath = os.path.join(
            local_mainpath, "scenario_definition", "emission_limits",
            "{}.json".format(self.scenario_definition["GHGgoals_json"]))
        with open(GHGScenpath) as f:
            GHG_Goal_definition = json.load(f)
        GHGlimit = pd.Series(GHG_Goal_definition["yearly_targets"])
        GHGlimit.index = [int(i) for i in GHGlimit.index]
        self.GHGlimit = GHGlimit[self.modelyears]
        print("\n GHG goals in optimization", flush=True)
        print(self.GHGlimit, flush=True)

        # Read and check input data and create scenario folder
        raw_input_data_paths = get_raw_input_data_paths(
            local_mainpath, self.scenario_definition)
        check_input_data(self.opt_parameters, raw_input_data_paths,
                         self.scenario_definition)
        check_renewable_input(self.scenario_definition, raw_input_data_paths)
        paths = folder_creation(local_mainpath, raw_input_data_paths,
                                self.scenario_definition, scenariopath,
                                GHGScenpath,
                                optParapath)
        self.resultfolderpath = paths["resfolderpath"]
        self.databasepath = paths["parameterfile"]
        self.inputprofiledatapath = paths["input_profiles"]
        self.outputprofiledatapath = paths["output_profiles"]
        self.historicaldatapath = paths["historicaldata"]
        self.forceddecommissioningdatapath = paths["forceddecommissioningdata"]
        self.heatloaddatapath = paths["heatload"]
        self.temppath = paths["temppath"]
        self.evaluationtemplatepath = raw_input_data_paths["template_evaluation"]
        check_input_data(self.opt_parameters, paths, self.scenario_definition)

    def get_first_parms(self):
        # Read some Scenario-Variables from json
        self.solver = self.scenario_definition["solver"]
        self.qp = bool(self.scenario_definition["QP"])
        if self.qp is True:
            self.qcpdual = 1
        else:
            self.qcpdual = 0
        self.default_wacc = self.scenario_definition["WACC"]
        # DF of years of optimization
        self.modelyears = [i for i in range(
            self.scenario_definition["startyear"],
            self.scenario_definition["targetyear"] + 1,
            self.scenario_definition["interval"])]
        if len(self.modelyears) > 1:
            self.modelyears.insert(0, 2020)

        # DF of CO2 limits in optimization years
        self.co2ac = pd.DataFrame(index=self.modelyears)
        self.co2ac['Abatement Costs [€/t]'] = 0

        # read year data
        self.refyear = self.scenario_definition["referenceyear"]
        self._startyear = self.scenario_definition["startyear"]
        self.targetyear = self.scenario_definition["targetyear"]
        self.interval = self.scenario_definition["interval"]
        self.typdays = self.scenario_definition["typdays"]
        self.modelyears = range(
            self.refyear, self.targetyear + 1, self.interval)

        self.MaxExpansion = self.opt_parameters["Max_Expansion"]
        self.MaxDecommissioning = self.opt_parameters["Max_Decomm"]
        self.startDiffusion = self.opt_parameters["Start_Diffusion"]
        # self.devFactor = self.opt_parameters["devFactor"]
        # self.MaxAddDev = self.opt_parameters["devAdd"]
        # self.firstYC = self.opt_parameters["firstYC"]
        self.considerMaxYearlyExpansionLimit = self.scenario_definition[
            "considerMaxYearlyExpansionLimit"]
        self.maxYearlyExpansionGW_Onshore = self.scenario_definition[
            "maxYearlyExpansionGW_Onshore"]
        self.maxYearlyExpansionGW_Offshore = self.scenario_definition[
            "maxYearlyExpansionGW_Offshore"]
        self.maxYearlyExpansionGW_OFPV = self.scenario_definition["maxYearlyExpansionGW_OFPV"]
        self.maxYearlyExpansionGW_RTPV = self.scenario_definition["maxYearlyExpansionGW_RTPV"]
        self.maxRefurbishmentRate = self.scenario_definition["maxRefurbRate"]
        self.sCurveParam = self.scenario_definition["sCurveParam"]
        self.BEVstoragefactor = self.scenario_definition["UsableBEVStorageCapacity_per_km"]
        self.string_identifiers = self.scenario_definition["string_identifiers"]

    def read_data_parameterfile(self):
        self.raw_sources = pd.read_excel(
            self.databasepath, sheet_name='Sources', index_col=0)
        self.raw_sinks = pd.read_excel(
            self.databasepath, sheet_name='Sinks', index_col=0)
        self.raw_storages = pd.read_excel(
            self.databasepath, sheet_name='Storages', index_col=0)
        self.raw_transformers = pd.read_excel(
            self.databasepath, sheet_name='Transformers', index_col=0)
        self.raw_hubs = pd.read_excel(
            self.databasepath, sheet_name='Hubs', index_col=0)
        self.transport = pd.read_excel(
            self.databasepath, sheet_name='Transport', index_col="Vehicle")
        self.transformers = pd.read_excel(
            self.databasepath, sheet_name='Transformers', index_col="name")
        self.raw_heatpumps = pd.read_excel(
            self.databasepath, sheet_name='HeatPumps', index_col=0)
        self.raw_fuelprices = pd.read_excel(
            self.databasepath, sheet_name='Fuel Prices', index_col=0)
        self.raw_demand = pd.read_excel(
            self.databasepath, sheet_name='Demand', index_col=0)
        self.raw_connections = pd.read_excel(
            self.databasepath, sheet_name='Connectors')

        self.raw_all_components = pd.concat(
            [self.raw_sources, self.raw_sinks, self.raw_storages, self.raw_hubs, self.raw_heatpumps])

        self.buildingdata = pd.read_excel(
            self.databasepath, sheet_name='Buildings',
            usecols="A,H", index_col=None)
        if len(self.buildingdata.columns) > 1:
            self.buildingdata = self.buildingdata.set_index(
                self.buildingdata.columns[0])

    def read_timeseries(self):
        self.raw_heatload_timeseries = pd.read_csv(
            self.heatloaddatapath, index_col=0)
        self.raw_inputprofile_timeseries = pd.read_csv(
            self.inputprofiledatapath, index_col=0)
        self.raw_outputprofile_timeseries = pd.read_csv(
            self.outputprofiledatapath, index_col=0)

    def read_forced_decommissioning_and_historical_data(self):
        self.forceddecommissioning = pd.read_excel(
            self.forceddecommissioningdatapath, index_col=0)
        self.historicalcapacity = pd.read_excel(
            self.historicaldatapath, index_col=0)

    def get_component_parameters_over_transformationpathway(self):
        """Initialize parameters for all years by interpolating between years
        in parameter file."""
        # 1. fuel prices
        dfFuels = self.raw_fuelprices.rename(
            columns={"Price20": 2020, "Price30": 2030, "Price40": 2040,
                     "Price50": 2050})
        dfFuels = dfFuels.loc[:, [2020, 2030, 2040, 2050]]
        new_years = [x for x in self.modelyears if x not in dfFuels.columns]
        # ugly implementation needed for old enercore-nestor environment
        for i in new_years:
            dfFuels[i] = np.NaN
        self.fuelprices = dfFuels.sort_index(axis=1).interpolate(axis=1)

        # 2. demand
        dfDemand = self.raw_demand.rename(
            columns={"2020": 2020, "2030": 2030, "2040": 2040, "2050": 2050})
        dfDemand = dfDemand.loc[:, [2020, 2030, 2040, 2050]]
        new_years = [x for x in self.modelyears if x not in dfDemand.columns]
        # ugly implementation needed for old enercore-nestor environment
        for i in new_years:
            dfDemand[i] = np.NaN
        self.demand = dfDemand.sort_index(axis=1).interpolate(axis=1)

        # 3. emissions
        emissions = pd.DataFrame()
        emissions[2020] = self.transformers['CO2footprint2020']
        emissions[2050] = self.transformers['CO2footprint2050']
        new_years = [x for x in self.modelyears if x not in emissions.columns]
        # ugly implementation needed for old enercore-nestor environment
        for i in new_years:
            emissions[i] = np.NaN
        self.emissions = emissions.sort_index(axis=1).interpolate(axis=1)

        # 4.capex
        capex_cols = ["capex20", "capex30", "capex40", "capex50"]
        capex = pd.concat(
            [x for x in [self.raw_sources[capex_cols],
                         self.raw_storages[capex_cols],
                         self.transformers[capex_cols],
                         self.raw_heatpumps[capex_cols]] if not x.empty],
            axis=0)
        # rename
        capex = capex.rename(
            columns={"capex20": 2020,
                     "capex30": 2030,
                     "capex40": 2040,
                     "capex50": 2050})
        new_years = [x for x in self.modelyears if x not in capex.columns]
        # ugly implementation needed for old enercore-nestor environment
        for i in new_years:
            capex[i] = np.NaN
        self.capex = capex.sort_index(axis=1).interpolate(axis=1)

        # 5. opex per Capacity
        self.opexPerCapacity = pd.DataFrame()
        # TODO later

        # 6. cost scale
        costscale_cols = ["cost_scale2020", "cost_scale2050"]
        costscale = pd.concat(
            [x for x in [self.raw_sources[costscale_cols],
                         self.raw_storages[costscale_cols],
                         self.transformers[costscale_cols],
                         self.raw_heatpumps[costscale_cols]] if not x.empty],
            axis=0)
        costscale = costscale.rename(
            columns={"cost_scale2020": 2020, "cost_scale2050": 2050})
        new_years = [x for x in self.modelyears if x not in costscale.columns]
        # ugly implementation needed for old enercore-nestor environment
        for i in new_years:
            costscale[i] = np.NaN
        self.costscale = costscale.sort_index(axis=1).interpolate(axis=1)

        # 7. efficency
        dfConnections = self.raw_connections.rename(
            columns={"efficiency20": 2020,
                     "efficiency30": 2030,
                     "efficiency40": 2040,
                     "efficiency50": 2050})
        dfConnections = dfConnections.set_index(["input", "output"])
        dfConnections = dfConnections.loc[:, [2020, 2030, 2040, 2050]]
        new_years = [
            x for x in self.modelyears if x not in dfConnections.columns]
        # ugly implementation needed for old senercore-nestor environment
        for i in new_years:
            dfConnections[i] = np.NaN
        connections = dfConnections.sort_index(
            axis=1).interpolate(axis=1).reset_index()

        connection_components = (
            [x for x in self.transformers.index]
            + [x for x in self.raw_heatpumps.index]
            + [x for x in self.raw_storages.index])

        self.dimEnergyType = {}
        self.efficiency = {}
        for component in connection_components:
            if isinstance(component, fn.Storage):
                pass
            self.efficiency[component] = {}
            for year in list(self.modelyears):
                self.efficiency[component][year] = {}
                # 1. get connections of component
                out_connections = connections.loc[
                    connections['input'] == component].set_index("output")
                in_connections = connections.loc[
                    connections['output'] == component].set_index("input")

                # 2. create empty efficiency dataframes
                in_efficiency = pd.DataFrame(index=in_connections.index)
                out_efficiency = pd.DataFrame(
                    index=out_connections.index)

                # 3. get energytype and efficiency of connecting component
                for in_comp in in_efficiency.index:
                    in_efficiency.loc[in_comp, 'energytype'] = \
                        self.raw_all_components.loc[in_comp, "energytype"]
                    in_efficiency.loc[in_comp, 'efficiency'] = \
                        in_connections.loc[in_comp, year]
                for out_comp in out_efficiency.index:
                    out_efficiency.loc[out_comp, 'energytype'] = \
                        self.raw_all_components.loc[out_comp, "energytype"]
                    out_efficiency.loc[out_comp, 'efficiency'] = \
                        out_connections.loc[out_comp, year]

                # 4. dimEnergytype
                # 4.1 get dimenergytype
                _found = False
                for df in [self.transformers, self.raw_sources, self.raw_storages, self.raw_heatpumps]:
                    if component in df.index:
                        _df = df
                        _found = True
                if _found == False:
                    raise ValueError(
                        f"Cannot find dimEnergyType for '{component}")
                if "dimEnergyType" in _df.columns:
                    filter = "dimEnergyType"
                else:
                    filter = "energytype"
                dimEnergy = _df.loc[component, filter]

                # 4.2 check if dimEnergy/reference commodity is in input or output
                if (dimEnergy in in_efficiency.index) and (dimEnergy in out_efficiency.index):
                    raise KeyError("Reference commodity in input and output for" +
                                   i.name)
                elif dimEnergy in list(in_efficiency["energytype"]):
                    out_referenceCommodity = False
                    dimHub = in_efficiency.loc[
                        in_efficiency["energytype"] == dimEnergy].index[0]
                    dimEfficiency = in_efficiency.loc[dimHub, "efficiency"]
                    self.efficiency[component][year][dimHub] = -1
                else:
                    out_referenceCommodity = True
                    dimHub = out_efficiency.loc[
                        out_efficiency["energytype"] == dimEnergy].index[0]
                    dimEfficiency = out_efficiency.loc[dimHub, "efficiency"]
                    self.efficiency[component][year][dimHub] = +1

                # excurse: get dimensioning Hub
                self.dimEnergyType[component] = dimHub

                # 5. write output conversions set other conversions for output
                for out_connection, out_connect_params in out_efficiency.iterrows():
                    # only write if the connection is not the reference connection
                    if out_connection == dimHub and not out_referenceCommodity:
                        continue

                    # get conversion factors for commodity connection of input
                    # component if dimEnergy in output
                    if out_referenceCommodity:
                        self.efficiency[component][year][out_connection] = \
                            out_connect_params.efficiency / \
                            dimEfficiency
                    # get conversion factors for commodity connection of input component if dimEnergy in input
                    # only take those i.efficiencies, where input is dimEnergy since there might be more than one input.
                    # nothing else needed since i.efficiencies are always
                    # dimensioned to input
                    else:
                        self.efficiency[component][year][out_connection] = \
                            out_connect_params.efficiency

                # 3. write input conversions
                for in_connection, in_connect_param in in_efficiency.iterrows():
                    # only write if the connection is not the reference connection
                    if in_connection == dimHub and out_referenceCommodity:
                        continue
                    if out_referenceCommodity:
                        self.efficiency[component][year][in_connection] = \
                            - in_connect_param.efficiency/dimEfficiency
                    else:
                        self.efficiency[component][year][in_connection] = \
                            -1 * dimEfficiency * in_connect_param.efficiency

    def define_optimizaton_specs(self):
        if self.solver == "gurobi":
            self.optimization_specs = (
                'BarHomogeneous={} '.format(str(self.opt_parameters["BarHomogeneous"])) +
                'tee={} '.format(str(bool(self.opt_parameters["tee"]))) +
                'NodeMethod={} '.format(str(self.opt_parameters["NodeMethod"])) +
                'Method={} '.format(str(self.opt_parameters["Method"])) +
                'Crossover={} '.format(str(self.opt_parameters["Crossover"])) +
                'NumericFocus={} '.format(self.opt_parameters["Numeric_Focus"]) +
                'QCPDual={} '.format(str(self.qcpdual)) +
                'getDual={} '.format(str(bool(self.opt_parameters["getDual"]))) +
                'OptimalityTol={}'.format(str(float('1e-' + str(self.opt_parameters["Opt_Tolerance"])))))
        elif self.solver == "glpk":
            # TODO implement optimization specs for glpk
            self.optimization_specs = ""

    def initalize_backcasting_network(self,):
        """Initialize the enery system model in FINE.

        Parameters
        ----------
        databasepath : str
            path to Energiesystem parameter file
        """
        commodity_list = list(self.raw_hubs.index)

        for i in self.raw_sources.index:
            for l in self.raw_connections.index:
                if i == self.raw_connections.loc[l, 'input'] and self.raw_sources.loc[i, 'CO2footprint'] != 0:
                    CO2in = self.raw_connections.loc[l, 'output'] + 'VirtIn'
                    commodity_list.append(CO2in)
            if self.string_identifiers["controlunit"] in i:
                commodity_list.append(i)

        for i in self.raw_sinks.index:
            for k in self.raw_connections.index:
                if i == self.raw_connections.loc[k, 'output'] and self.raw_sinks.loc[i, 'CO2footprint'] != 0:
                    CO2out = self.raw_connections.loc[k, 'input'] + 'VirtOut'
                    commodity_list.append(CO2out)

        commodity_list.append('CO2Out')
        commodity_list = list(set(commodity_list))

        # Initialize an ESM with FINE
        self.location = 'GermanyRegion'
        self.locations = {self.location}

        # FINE needs commodity-dict {commodity:unit}, for now {commodity:commodity}
        commodityUnitDict = dict()
        for commodity in commodity_list:
            commodityUnitDict[commodity] = str(commodity)

        self.esM = fn.EnergySystemModel(locations=self.locations,
                                        commodities=set(commodity_list),
                                        numberOfTimeSteps=8760,
                                        commodityUnitsDict=commodityUnitDict,
                                        hoursPerTimeStep=1, costUnit='1e9 Euro',
                                        lengthUnit='km', verboseLogLevel=0)
        self.components_in_network()

    def components_in_network(self):
        """Add components to FINE esM."""
        if self.qp is False:  # check
            for df in [self.raw_connections, self.raw_sources,
                       self.raw_transformers, self.raw_sinks,
                       self.raw_heatpumps, self.raw_storages]:
                # richtiger str?
                _cols = [x for x in df.columns if "cost_scale" in x]
                df[_cols] = 0

        # FINE cannot handle Transformers in both directions - ignore rsofc
        rsofc_string = [x for x in self.raw_transformers.index if "rSOFC" in x]
        self.raw_transformers = self.raw_transformers.drop(
            rsofc_string, axis=0)

        def _get_temp_name(name):
            # for stocks, also "not-stock" name is needed for reading parameters
            if '_stock' in name:
                temp_name = name.replace("_stock", "")
            else:
                temp_name = name
            return temp_name

        # wacc of component
        def _get_wacc(name, params, temp_param):
            def _correct_wacc_info(name, wacc):
                if wacc is None:
                    pass
                else:
                    if type(wacc) is str and "None" in wacc:
                        wacc = None
                    elif type(wacc) is float:
                        pass
                    else:
                        raise TypeError(
                            "Check wacc input format for {}.".format(name)+" It has type {}".format(type(wacc)))

                return wacc

            params.WACC = _correct_wacc_info(name, params.WACC)
            temp_param.WACC = _correct_wacc_info(name, temp_param.WACC)

            # get WACC for either name, temp_name or set default
            if params.WACC is not None:
                wacc = params.WACC
            elif temp_param.WACC is not None:
                wacc = temp_param.WACC
            else:
                wacc = self.default_wacc
            return wacc

        # add components to network
        # 1. Transformers
        for transformer, transformer_params in self.raw_transformers.iterrows():
            transformer_temp_name = _get_temp_name(transformer)
            transformer_wacc = _get_wacc(
                transformer,
                transformer_params,
                self.raw_transformers.loc[transformer_temp_name])
            if "stock" in transformer:
                conversionFactors = self.efficiency[transformer][2020]
                if self.emissions.loc[transformer, 2020] != 0:
                    conversionFactors['CO2Out'] = self.emissions.loc[
                        transformer, 2020]
            else:
                conversionFactors = self.efficiency[transformer][self.currentyear]
                if self.emissions.loc[transformer, self.currentyear] != 0:
                    conversionFactors['CO2Out'] = self.emissions.loc[
                        transformer, self.currentyear]

            # get capex and costscale values
            _transformer_capex = self.capex.loc[transformer, self.currentyear]
            # Add to esm Network, differ between stock and not-stock
            if '_stock' in transformer:
                capacityMax = None
                capacityMin = None
                capacityFix = float(transformer_params.ub)
                _transformer_costscale = None
            else:
                if float(transformer_params.ub) != float(transformer_params.lb):
                    capacityMax = float(transformer_params.ub)
                    capacityMin = float(transformer_params.lb)
                    capacityFix = None
                    if self.qp:
                        _transformer_costscale = self.costscale.loc[
                            transformer, self.currentyear]
                    else:
                        _transformer_costscale = None
                else:
                    capacityMax = None
                    capacityMin = None
                    capacityFix = float(transformer_params.lb)
                    _transformer_costscale = None
            if _transformer_costscale is not None and _transformer_costscale != 0:
                self.esM.add(
                    fn.Conversion(
                        esM=self.esM, name=transformer,
                        physicalUnit=self.dimEnergyType[transformer],
                        commodityConversionFactors=conversionFactors,
                        hasCapacityVariable=True,
                        capacityMax=capacityMax,
                        capacityMin=capacityMin,
                        capacityFix=capacityFix,
                        investPerCapacity=_transformer_capex,
                        opexPerOperation=transformer_params.opex_var,
                        opexPerCapacity=_transformer_capex * transformer_params.opex_fix,
                        QPcostScale=_transformer_costscale,
                        economicLifetime=transformer_params.lifetime,
                        operationRateFix=None,
                        interestRate=transformer_wacc))
            else:
                self.esM.add(
                    fn.Conversion(
                        esM=self.esM, name=transformer,
                        physicalUnit=self.dimEnergyType[transformer],
                        commodityConversionFactors=conversionFactors,
                        hasCapacityVariable=True,
                        capacityMax=capacityMax,
                        capacityMin=capacityMin,
                        capacityFix=capacityFix,
                        investPerCapacity=_transformer_capex,
                        opexPerOperation=transformer_params.opex_var,
                        opexPerCapacity=_transformer_capex * transformer_params.opex_fix,
                        economicLifetime=transformer_params.lifetime,
                        operationRateFix=None,
                        interestRate=transformer_wacc))
        # 2. Sources
        for source, source_params in self.raw_sources.iterrows():
            source_temp_name = _get_temp_name(source)
            source_wacc = _get_wacc(
                source, source_params, self.raw_sources.loc[source_temp_name])

            if self.string_identifiers["controlunit"] in source:
                sourceEnergyType = source
            else:
                # try to load data for stock, if not existing load
                # connections from not-stock component
                comp_connection = \
                    self.raw_connections.loc[self.raw_connections["input"] == source]
                if len(comp_connection) == 0:
                    comp_connection = \
                        self.raw_connections.loc[
                            self.raw_connections["input"] == source_temp_name]
                    if len(comp_connection) == 0:
                        raise KeyError(
                            "Source {} has no connections".format(source))
                if len(comp_connection) != 1:
                    raise KeyError(
                        "Source {} too many connections".format(source))
                comp_connection = comp_connection.squeeze()

                if source_params.CO2footprint != 0:
                    # Add virtual conversion for CO2
                    # footprint in FINE as FINE-sources cannot produce CO2
                    sourceEnergyType = \
                        comp_connection['output'] + 'VirtIn'
                    conversionEnergyType = \
                        comp_connection['output']
                    conversionFactors = {}

                    # check that efficiencies are the same, otherwise code
                    # needs to be adapted
                    yearly_effficiencies = [
                        comp_connection[x] for x in comp_connection.index if x.startswith("efficiency")]
                    if all(x == yearly_effficiencies[0] for x in yearly_effficiencies):
                        efficiency = yearly_effficiencies[0]
                    else:
                        raise ValueError(
                            "Efficiencies of {} to {} vary between years. ".format(comp_connection["input"], comp_connection["output"]) +
                            "Yearly dependendy needs to be implemented here.")

                    if source_params.CO2footprint > 0:
                        conversionFactors[sourceEnergyType] = \
                            - efficiency / source_params.CO2footprint
                        conversionFactors[conversionEnergyType] = \
                            + efficiency / source_params.CO2footprint
                        conversionFactors['CO2Out'] = 1
                    else:
                        conversionFactors[sourceEnergyType] = \
                            +  efficiency / source_params.CO2footprint
                        conversionFactors[conversionEnergyType] = \
                            - efficiency / source_params.CO2footprint
                        conversionFactors['CO2Out'] = - 1

                    self.esM.add(
                        fn.Conversion(
                            esM=self.esM,
                            name=source + 'Virt',
                            physicalUnit='CO2Out',
                            commodityConversionFactors=conversionFactors,
                            hasCapacityVariable=True,
                            capacityMax=None,
                            capacityMin=None,
                            investPerCapacity=0,
                            opexPerOperation=0,
                            opexPerCapacity=0,
                            economicLifetime=source_params.lifetime,
                            interestRate=source_wacc))
                else:
                    sourceEnergyType = comp_connection['output']

            # Profiles of source
            if source_params["profile"] not in [None, "None"]:
                p1 = pd.Series(
                    self.raw_inputprofile_timeseries[source_params["profile"]])
                if not source_params.controllable:
                    operationRateFix = _round(p1)
                    operationRateMax = None
                else:
                    operationRateMax = _round(p1)
                    operationRateFix = None
            else:
                p1 = None
                if not source_params.controllable:
                    operationRateFix = pd.Series([1] * 8760)
                    operationRateMax = None
                else:
                    operationRateMax = pd.Series([1] * 8760)
                    operationRateFix = None

            # get parameters
            source_params.capex = self.capex.loc[source, self.currentyear]
            _source_costscale = self.costscale.loc[source, self.currentyear]

            # add source to esM
            if source_params.lb is None or source_params.ub is None:
                raise ValueError(f"Missing value for lb or ub for '{source}'")
            if '_stock' in source:
                capacityFix_Source = float(source_params.lb)
                capacityMax_Source = None
                capacityMin_Source = None
            else:
                # with min and max capacity if not min==max
                if float(source_params.ub) != float(source_params.lb):
                    capacityFix_Source = None
                    capacityMax_Source = float(source_params.ub)
                    capacityMin_Source = float(source_params.lb)
                else:
                    capacityFix_Source = float(source_params.lb)
                    capacityMax_Source = None
                    capacityMin_Source = None
                    _source_costscale = 0

            # add shared expansion ID for renewable expansion limits
            if self.considerMaxYearlyExpansionLimit is True:
                if source in self.string_identifiers["onshore"]:
                    sharedExpansionID = "OnshoreExpansion"
                    sharedCapacityExpansionMax = self.maxYearlyExpansionGW_Onshore
                    sharedCapacityExpansionMin = None
                elif source in self.string_identifiers["offshore"]:
                    sharedExpansionID = "OffshoreExpansion"
                    sharedCapacityExpansionMax = self.maxYearlyExpansionGW_Offshore
                    sharedCapacityExpansionMin = None
                elif source in self.string_identifiers["rooftop_pv"]:
                    sharedExpansionID = "RooftopPVExpansion"
                    sharedCapacityExpansionMax = self.maxYearlyExpansionGW_RTPV
                    sharedCapacityExpansionMin = None
                elif source in self.string_identifiers["openfield_pv"]:
                    sharedExpansionID = "OpenfieldPVExpansion"
                    sharedCapacityExpansionMax = self.maxYearlyExpansionGW_OFPV
                    sharedCapacityExpansionMin = None
                else:
                    sharedExpansionID = None
                    sharedCapacityExpansionMax = None
                    sharedCapacityExpansionMin = None
            else:
                sharedExpansionID = None
                sharedCapacityExpansionMax = None
                sharedCapacityExpansionMin = None

            if self.considerMaxYearlyExpansionLimit:
                self.esM.add(
                    fn.Source(
                        esM=self.esM, name=source,
                        commodity=sourceEnergyType,
                        operationRateMax=operationRateMax,
                        operationRateFix=operationRateFix,
                        hasCapacityVariable=True,
                        capacityFix=capacityFix_Source,
                        capacityMax=capacityMax_Source,
                        capacityMin=capacityMin_Source,
                        investPerCapacity=source_params.capex,
                        opexPerCapacity=source_params.capex * source_params.opex_fix,
                        opexPerOperation=float(source_params.opex_var),
                        sharedExpansionID=sharedExpansionID,
                        sharedCapacityExpansionMax=sharedCapacityExpansionMax,
                        sharedCapacityExpansionMin=sharedCapacityExpansionMin,
                        commodityCost=0,
                        commodityRevenue=0,
                        interestRate=source_wacc,
                        QPcostScale=_source_costscale,
                        economicLifetime=source_params.lifetime))
            else:
                self.esM.add(
                    fn.Source(
                        esM=self.esM, name=source,
                        commodity=sourceEnergyType,
                        operationRateMax=operationRateMax,
                        operationRateFix=operationRateFix,
                        hasCapacityVariable=True,
                        capacityFix=capacityFix_Source,
                        capacityMax=capacityMax_Source,
                        capacityMin=capacityMin_Source,
                        investPerCapacity=source_params.capex,
                        opexPerCapacity=source_params.capex * source_params.opex_fix,
                        opexPerOperation=float(source_params.opex_var),
                        commodityCost=0,
                        commodityRevenue=0,
                        interestRate=source_wacc,
                        QPcostScale=_source_costscale,
                        economicLifetime=source_params.lifetime))
        # 3. sinks
        for sink, sink_params in self.raw_sinks.iterrows():

            sink_temp_name = _get_temp_name(sink)
            sink_wacc = _get_wacc(
                sink, sink_params, self.raw_sinks.loc[sink_temp_name])

            comp_connection = \
                self.raw_connections.loc[self.raw_connections["output"] == sink]
            if len(comp_connection) == 0:
                # future stocks are not written as stock in
                # parameter-file -> use not-stock
                comp_connection = \
                    self.raw_connections.loc[
                        self.raw_connections["output"] == sink_temp_name]
                if len(comp_connection) == 0:
                    raise KeyError(
                        "Faulty naming of component '{}'".format(sink) +
                        " in parameter-file. Double check.")
            if len(comp_connection) != 1:
                raise KeyError(
                    "Sink {} has more than one connection".format(sink))
            comp_connection = comp_connection.squeeze()
            if sink_params.CO2footprint != 0:
                # Add virtual conversion for CO2 footprint
                sinkEnergyType = comp_connection['input'] + 'VirtOut'
                conversionEnergyType = comp_connection['input']
                conversionFactors = {}

                # check that efficiencies are the same, otherwise code
                # needs to be adapted
                yearly_effficiencies = [
                    comp_connection[x] for x in comp_connection.index if x.startswith("efficiency")]
                if all(x == yearly_effficiencies[0] for x in yearly_effficiencies):
                    efficiency = yearly_effficiencies[0]
                else:
                    raise ValueError(
                        "Efficiencies of {} to {} vary between years. ".format(comp_connection["input"], comp_connection["output"]) +
                        "Yearly dependendy needs to be implemented here.")

                if sink_params.CO2footprint > 0:
                    conversionFactors[sinkEnergyType] = \
                        + efficiency / sink_params.CO2footprint
                    conversionFactors[conversionEnergyType] = \
                        - efficiency / sink_params.CO2footprint
                    conversionFactors['CO2Out'] = 1
                else:
                    conversionFactors[sinkEnergyType] = \
                        - efficiency / sink_params.CO2footprint
                    conversionFactors[conversionEnergyType] = \
                        + efficiency / sink_params.CO2footprint
                    conversionFactors['CO2Out'] = -1

                self.esM.add(
                    fn.Conversion(
                        esM=self.esM, name=sink + 'Virt',
                        physicalUnit=sinkEnergyType,
                        commodityConversionFactors=conversionFactors,
                        hasCapacityVariable=True,
                        capacityMax=None,
                        capacityMin=None,
                        investPerCapacity=0,
                        opexPerOperation=0,
                        opexPerCapacity=0,
                        economicLifetime=sink_params.lifetime,
                        interestRate=sink_wacc))
            else:
                sinkEnergyType = comp_connection['input']

            if sink_params.opex_var < 0:
                commodityCost = 0
                commodityRevenue = sink_params.opex_var * -1
            else:
                commodityCost = sink_params.opex_var
                commodityRevenue = 0

            if sink_params["profile"] not in [None, "None"]:
                p1 = pd.Series(
                    self.raw_outputprofile_timeseries[sink_params["profile"]])
                if not sink_params.controllable:
                    operationRateFix = _round(p1)
                    operationRateMax = None
                else:
                    operationRateMax = _round(p1)
                    operationRateFix = None
            else:
                p1 = None
                if not sink_params.controllable:
                    operationRateFix = pd.Series([1] * 8760)
                    operationRateMax = None
                else:
                    operationRateMax = pd.Series([1] * 8760)
                    operationRateFix = None

            # add sink with either flexible or fixed capacity
            if sink_params.ub != sink_params.lb:
                capacityFix_Sink = None
                capacityMax_Sink = float(sink_params.ub)
                capacityMin_Sink = float(sink_params.lb)
            else:
                capacityFix_Sink = float(sink_params.lb)
                capacityMax_Sink = None
                capacityMin_Sink = None
            self.esM.add(
                fn.Sink(
                    esM=self.esM, name=sink, commodity=sinkEnergyType,
                    operationRateMax=operationRateMax,
                    operationRateFix=operationRateFix,
                    hasCapacityVariable=True,
                    capacityFix=capacityFix_Sink,
                    capacityMax=capacityMax_Sink,
                    capacityMin=capacityMin_Sink,
                    investPerCapacity=sink_params.capex,
                    opexPerCapacity=sink_params.opex_fix * sink_params.capex,
                    opexPerOperation=0,
                    commodityCost=commodityCost,
                    commodityRevenue=commodityRevenue,
                    interestRate=sink_wacc,
                    economicLifetime=sink_params.lifetime))
        # 4. Storages
        for storage, storage_params in self.raw_storages.iterrows():
            storage_temp_name = _get_temp_name(storage)
            storage_wacc = _get_wacc(
                storage, storage_params, self.raw_storages.loc[storage_temp_name])

            storageEnergyType = self.raw_connections.loc[
                self.raw_connections['input'] == storage, 'output']
            if len(storageEnergyType) == 0:
                storageEnergyType = self.raw_connections.loc[
                    self.raw_connections['input'] == storage_temp_name, 'output']
            if len(storageEnergyType) == 1:
                storageEnergyType = storageEnergyType.iloc[0]
            else:
                raise KeyError(
                    "Connections of storage {} problematic".format(storage))

            # get params
            _storage_capex = self.capex.loc[storage, self.currentyear]
            if storage == self.string_identifiers["BatteryElectricCar_Storage"]:
                capacityFix_Storage = 0
                capacityMax_Storage = None
                capacityMin_Storage = None
            elif '_stock' in storage:
                capacityFix_Storage = float(storage_params.ub)
                capacityMax_Storage = None
                capacityMin_Storage = None
            else:
                if float(storage_params.ub) != float(storage_params.lb):
                    capacityFix_Storage = None
                    capacityMax_Storage = float(storage_params.ub)
                    capacityMin_Storage = float(storage_params.lb)
                else:
                    capacityFix_Storage = float(storage_params.ub)
                    capacityMax_Storage = None
                    capacityMin_Storage = None
            self.esM.add(
                fn.Storage(
                    esM=self.esM, name=storage,
                    commodity=storageEnergyType,
                    hasCapacityVariable=True,
                    capacityFix=capacityFix_Storage,
                    capacityMax=capacityMax_Storage,
                    capacityMin=capacityMin_Storage,
                    investPerCapacity=_storage_capex,
                    opexPerCapacity=_storage_capex * storage_params.opex_fix,
                    opexPerChargeOperation=storage_params.opex_var,
                    chargeEfficiency=storage_params.chargeEfficiency,
                    dischargeEfficiency=storage_params.dischargeEfficiency,
                    selfDischarge=storage_params.selfDischarge,
                    interestRate=storage_wacc,
                    economicLifetime=storage_params.lifetime))
        # 5 Heatpumps
        for heatpump, heatpump_params in self.raw_heatpumps.iterrows():
            heatpump_temp_name = _get_temp_name(heatpump)
            heatpump_wacc = _get_wacc(
                heatpump, heatpump_params,
                self.raw_heatpumps.loc[heatpump_temp_name])

            dfTempOut = self.raw_connections.loc[self.raw_connections['input'] == heatpump]
            dfTempIn = self.raw_connections.loc[self.raw_connections['output'] == heatpump]
            # if no information about stock  found, look in not-stock
            if len(dfTempOut) == 0 and len(dfTempIn) == 0:
                dfTempOut = self.raw_connections.loc[
                    self.raw_connections['input'] == heatpump_temp_name]
                dfTempIn = self.raw_connections.loc[
                    self.raw_connections['output'] == heatpump_temp_name]
                if len(dfTempOut) == 0 and len(dfTempIn) == 0:
                    raise KeyError(
                        "Faulty naming of component " +
                        "'{}' in parameter-file. Double check.".format(heatpump))
            if len(dfTempOut) > 1 or len(dfTempIn) > 1:
                raise KeyError("Too many connections for " +
                               "heatpump {}".format(heatpump))

            dfTempIn = dfTempIn.squeeze()
            dfTempOut = dfTempOut.squeeze()

            if self.string_identifiers["geothermalHeatPumps"] in heatpump:
                temperature_ts = self.raw_heatload_timeseries["Geothermal_Temperature"]
            else:
                temperature_ts = self.raw_heatload_timeseries["Temperature"]

            conversionFactors_HP = get_heatpump_conversion(
                efficiency=heatpump_params.efficiency,
                T_hot=heatpump_params.T_hot,
                T_cold_time_series=temperature_ts,
                T_limit=heatpump_params.T_limit,
                input_name=dfTempIn['input'],
                output_name=dfTempOut['output'])

            physicalUnit = dfTempOut['output']

            # get params
            _heatpump_capex = self.capex.loc[heatpump, self.currentyear]
            if '_stock' in heatpump:
                capacityFix_HP = heatpump_params.ub
                capacityMax_HP = None
                capacityMin_HP = None
                _heatpump_costscale = None

            else:
                if heatpump_params.ub != heatpump_params.lb:
                    capacityFix_HP = None
                    capacityMax_HP = float(heatpump_params.ub)
                    capacityMin_HP = float(heatpump_params.lb)
                    if self.qp:
                        _heatpump_costscale = self.costscale.loc[
                            transformer, self.currentyear]
                    else:
                        _heatpump_costscale = None

                else:
                    capacityFix_HP = heatpump_params.ub
                    capacityMax_HP = None
                    capacityMin_HP = None
                    _heatpump_costscale = None

            if _heatpump_costscale is not None and _heatpump_costscale != 0:
                self.esM.add(
                    fn.Conversion(
                        esM=self.esM, name=heatpump,
                        physicalUnit=physicalUnit,
                        commodityConversionFactors=conversionFactors_HP,
                        hasCapacityVariable=True,
                        capacityFix=capacityFix_HP,
                        capacityMax=capacityMax_HP,
                        capacityMin=capacityMin_HP,
                        investPerCapacity=_heatpump_capex,
                        opexPerOperation=heatpump_params.opex_var,
                        opexPerCapacity=_heatpump_capex * heatpump_params.opex_fix,
                        economicLifetime=heatpump_params.lifetime,
                        interestRate=heatpump_wacc,
                        QPcostScale=heatpump_params.costscale))
            else:
                self.esM.add(
                    fn.Conversion(
                        esM=self.esM, name=heatpump,
                        physicalUnit=physicalUnit,
                        commodityConversionFactors=conversionFactors_HP,
                        hasCapacityVariable=True,
                        capacityFix=capacityFix_HP,
                        capacityMax=capacityMax_HP,
                        capacityMin=capacityMin_HP,
                        investPerCapacity=_heatpump_capex,
                        opexPerOperation=heatpump_params.opex_var,
                        opexPerCapacity=_heatpump_capex * heatpump_params.opex_fix,
                        economicLifetime=heatpump_params.lifetime,
                        interestRate=heatpump_wacc))

        # Add CO2 sink and limit
        self.esM.add(
            fn.Sink(
                esM=self.esM, name='CO2Environment',
                commodity='CO2Out', commodityLimitID='CO2_cap',
                hasCapacityVariable=False,
                yearlyLimit=self.GHGlimit[self.currentyear],
                interestRate=self.default_wacc))


def get_heatpump_conversion(efficiency, T_hot, T_cold_time_series, T_limit,
                            input_name, output_name):
    conversionFactors = {}
    Hprofile = -1 / (efficiency * (T_hot + 273.15) /
                     (T_hot - T_cold_time_series))
    Hprofile[T_cold_time_series < T_limit] = 0.0
    TDconvIn = pd.Series(Hprofile)
    TDconvOut = pd.Series(np.array([1] * 8760))
    conversionFactors[output_name] = TDconvOut.reset_index(drop=True)
    conversionFactors[input_name] = TDconvIn.reset_index(drop=True)
    return conversionFactors


if __name__ == "__main__":
    Nestor("newTHG0")
