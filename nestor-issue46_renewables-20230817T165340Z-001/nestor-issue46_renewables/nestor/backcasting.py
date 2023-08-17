import numpy as np
import pandas as pd
import os
import sys
import shutil
import FINE as fn

from nestor.add_comp import update_parameter, addConversion, addSource, addSink, addStorage
from nestor.utils.exportResult import get_yearly_FINE_results, export_FINE_results
from nestor.utils.calcDemand import calculate_demand
from nestor.utils.check_output import check_output
from nestor.utils.data_handling import create_evaluation_file


class MyopicTrans():
    """Class which manages the myopic transformation path analysis"""

    def __init__(self, parent):
        """Initialize the MyopicTrans class."""
        self.parent = parent

        # get raw ub from parameter files for later comparisons
        self.raw_ub = pd.concat([
            self.parent.transformers["ub"],
            self.parent.raw_sources["ub"],
            self.parent.raw_heatpumps["ub"],
            self.parent.raw_hubs["ub"],
            self.parent.raw_storages["ub"],
            self.parent.raw_sinks["ub"]], axis=0)
        self.raw_ub = _round(self.raw_ub)

        self.raw_lb = pd.concat([
            self.parent.transformers["lb"],
            self.parent.raw_sources["lb"],
            self.parent.raw_heatpumps["lb"],
            self.parent.raw_hubs["lb"],
            self.parent.raw_storages["lb"],
            self.parent.raw_sinks["lb"]], axis=0)
        self.raw_lb = _round(self.raw_lb)

        self.raw_opexFix = pd.concat([
            self.parent.transformers["opex_fix"],
            self.parent.raw_sources["opex_fix"],
            self.parent.raw_heatpumps["opex_fix"],
            self.parent.raw_hubs["opex_fix"],
            self.parent.raw_storages["opex_fix"]], axis=0)
        self.raw_opexFix = _round(self.raw_opexFix)

        self.helper_capex_calculation = {}
        self.resultCapacities = {}

        # get unrestricted components
        self.unrestricted_components = pd.concat([
            self.parent.transformers.loc[
                self.parent.transformers["unrestrict"] == True],
            self.parent.raw_sources.loc[self.parent.raw_sources["unrestrict"] == True],
            self.parent.raw_storages.loc[
                self.parent.raw_storages["unrestrict"] == True],
            self.parent.raw_sinks.loc[self.parent.raw_sinks["unrestrict"] == True],
            self.parent.raw_hubs.loc[self.parent.raw_hubs["unrestrict"] == True]],
            axis=0).index

        # initialize empty temp files
        self.initialize_temp_files()

    def transformation_pathway(self):

        #######################################################################
        # Target Year
        #######################################################################
        print('\n' + 'Target Year', flush=True)

        self.initialize_optimization_year(
            currentyear=self.parent.targetyear,
            targetyear_optimization=True)

        # Optimization
        print('\n' + 'optimizing Target Year...', flush=True)
        if self.parent.typdays is not None:
            self.parent.esM.cluster(numberOfTypicalPeriods=self.parent.typdays)
            tsa = True
        else:
            tsa = False

        self.parent.esM.optimize(
            timeSeriesAggregation=tsa, solver=self.parent.solver,
            optimizationSpecs=self.parent.optimization_specs)
        check_for_infeasibility(
            str(self.parent.esM.solverSpecs["terminationCondition"]), year="Target Year")
        print('\n' + 'Target Year optimized!', flush=True)

        # save results and transform to enercore-network
        # TODO make new approach!

        print('saving results...', flush=True)
        FINEResults = {}
        FINEResults = get_yearly_FINE_results(
            FINEResults=FINEResults, esM=self.parent.esM, year="target year")
        print('results saved!')

        # perform backcasting
        print('perform backcasting...', flush=True)
        self.initBackcasting()
        print('backcasting performed!', flush=True)

        ##########################################################################
        # Myopic Transformation Path
        ##########################################################################
        # other years
        # TODO if above with range initialized, change here to >1
        if len(self.parent.modelyears) > 2:
            for year in self.parent.modelyears:
                print('\n \n \n Year: ' + str(year))
                # get technological parameters
                self.initialize_optimization_year(currentyear=year)

                # Optimization
                if self.parent.typdays is not None:
                    self.parent.esM.cluster(
                        numberOfTypicalPeriods=self.parent.typdays)
                print('\n' + 'optimizing ' + str(year), flush=True)
                self.parent.esM.optimize(
                    timeSeriesAggregation=tsa, solver=self.parent.solver,
                    optimizationSpecs=self.parent.optimization_specs)

                check_for_infeasibility(
                    str(self.parent.esM.solverSpecs["terminationCondition"]), year=self.parent.currentyear)

                print('\n' + str(year) + ' optimized!',
                      flush=True)

                # Export results and transform to enercore-network
                # TODO make new with FINE
                print('saving results...', flush=True)
                FINEResults = get_yearly_FINE_results(
                    FINEResults=FINEResults, esM=self.parent.esM, year=self.parent.currentyear)
                FINEResults = calculate_demand(
                    self.parent.currentyear, self.parent.resultfolderpath,
                    FINEResults, self.parent.esM)
                print('results saved!')
                print('\n Finished' + str(year), flush=True)

        ##########################################################################
        # Export Transformation path analysis results
        ##########################################################################
        resultfilepath = self.parent.resultfolderpath + '/Transformation_overview.xlsx'
        export_FINE_results(anyears=self.parent.modelyears, FINEResults=FINEResults,
                            esM=self.parent.esM, resultfilepath=resultfilepath)

        ##########################################################################
        # Check output of optimization
        ##########################################################################
        check_output(databasepath=self.parent.databasepath,
                     historicaldatapath=self.parent.historicaldatapath,
                     resultfilepath=resultfilepath,
                     targetyear=self.parent.scenario_definition["targetyear"])

        ##########################################################################
        # Result data handling
        ##########################################################################
        # Auswerte-File (Copyfrom transformation_overview to Auswerte-File)
        create_evaluation_file(
            resultfolderpath=self.parent.resultfolderpath,
            resultfilepath=resultfilepath,
            templateEvaluationPath=self.parent.evaluationtemplatepath,
            scenario_definition=self.parent.scenario_definition)

        # For Cluster-Runs: copy log file to results folder
        try:
            log_file_name = sys.argv[2]
            local_mainpath = os.path.join(os.path.dirname(__file__), "data")
            log_path = os.path.join(
                os.path.abspath(os.path.join(local_mainpath, "../..")),
                log_file_name)
            shutil.copy(log_path, os.path.join(self.parent.resultfolderpath))
        except:
            pass

        ##########################################################################
        # Finished
        ##########################################################################
        # delete temp data
        # shutil.rmtree(temppath, ignore_errors=True)
        print('\n' + 'Completed!', flush=True)

    def initialize_optimization_year(
            self, currentyear, targetyear_optimization=False):
        # initialize start and current year of optimization
        if targetyear_optimization is True:
            self.parent.startyear = self.parent.targetyear
            self.parent.currentyear = self.parent.targetyear
            self.parent.targetyearoptimization = True

        else:
            self.parent.startyear = self.parent._startyear
            self.parent.currentyear = currentyear
            self.parent.targetyearoptimization = False

        # utils for numerical stability
        self.round_components_in_network()

        # CAPACITY FUNCTIONS
        # 0. get resulting capacities of previous year
        if not targetyear_optimization and not self.parent.currentyear == self.parent.refyear:
            self.getOptimalCapacities()
        # 1. Create New Stock in Network
        print('Creating new stocks in network...', flush=True)
        self.createNewStockInNetwork()
        print('New Stocks created!', flush=True)
        # 2. Update Stock capacities
        print('Calculate stock capacities...', flush=True)
        self.calculateStockCapacities()
        print('Stock capacities created!', flush=True)

        # 4. update upper and lower bounds
        if not targetyear_optimization and not self.parent.currentyear == self.parent.refyear:
            print("Calculate New techs...", flush=True)
            self.calculateNewTechs()
            print("New techs calulated!", flush=True)

        # PARAMETER FUNCTIONS
        print("Update stock parameters...", flush=True)
        self.updateStockParameters()
        print("Stock parameters updated!", flush=True)
        print("Update technology parameters...", flush=True)
        self.updateTechnologyParameters()
        print("Technology parameters updated!", flush=True)

        # DEMAND
        print('calculating demand...', flush=True)
        self.calculateDemand()
        print('demand calculated!', flush=True)

        # GHG limits
        print('update GHG limits...', flush=True)
        self.updateGHGlimits()
        print('GHG updated', flush=True)

        # RESTRICT RENEWABLES
        if self.parent.considerMaxYearlyExpansionLimit:
            self.limitRenewableExpansion()
            self.checkRenewableExpansionLimit()

        # check data
        for component_name, mdl in self.parent.esM.componentNames.items():
            component = self.parent.esM.getComponent(component_name)
            ub = component.capacityMax
            lb = component.capacityMin
            fix = component.capacityFix
            if (ub is not None and fix is not None) or (lb is not None and fix is not None):
                raise ValueError(
                    f"Capacity Min and Capacity Fixis defined for {component.name}")

    def checkRenewableExpansionLimit(self):
        cap_fix = pd.Series()
        cap_max = pd.Series()
        cap_min = pd.Series()
        shared_expansion_min = {}
        shared_expansion_max = {}
        if self.parent.currentyear != self.parent.refyear:
            for tech in ["onshore", "offshore", "openfield_pv", "rooftop_pv"]:
                cap_min[tech] = 0
                cap_max[tech] = 0
                cap_fix[tech] = 0

                for comp in self.parent.esM.componentNames:
                    component = self.parent.esM.getComponent(comp)
                    if self.parent.string_identifiers[tech] in component.name:
                        if "stock" in component.name:
                            continue
                        # get shared expansion values
                        shared_expansion_min[tech] = component.sharedCapacityExpansionMin
                        shared_expansion_max[tech] = component.sharedCapacityExpansionMax

                        if component.capacityMin is None:
                            pass
                        else:
                            cap_min[tech] += component.capacityMin[self.parent.location]
                        if component.capacityMax is None:
                            cap_fix[tech] += component.capacityFix[self.parent.location]
                        else:
                            cap_max[tech] += component.capacityMax[self.parent.location]

                # check that limits are kept and will not to infeasible
                # problems
                # TODO installierte leistung weil die später rausfallen könnte
                # zu infeasible werden
                if shared_expansion_min[tech] is not None:
                    if cap_max[tech].sum() < shared_expansion_min[tech]:
                        raise ValueError(
                            "Renewable limitation will lead to an infeasbile " +
                            f"optimization. The capacityMax ({cap_max[tech].sum()}) " +
                            "of the components for " +
                            f"'{tech}' are lower than the sharedExpansionMin" +
                            f" ({shared_expansion_min[tech]})")
                if cap_min[tech].sum() > shared_expansion_max[tech]:
                    raise ValueError(
                        "Renewable limitation will lead to an infeasbile " +
                        "optimization. The capacityMin of the components for " +
                        f"'{tech}' are higher than the sharedExpansionMax")

    def limitRenewableExpansion(self):
        if self.parent.currentyear == self.parent.refyear:
            for tech in ["onshore", "offshore", "openfield_pv", "rooftop_pv"]:
                tech_items = [
                    x for x in self.parent.esM.componentNames if self.parent.string_identifiers[tech] in x and "_stock" not in x]
                for component in tech_items:
                    # update component
                    self.parent.esM = update_parameter(
                        self.parent.esM.getComponent(component),
                        self.parent.esM,
                        {"sharedExpansionID": None,
                         "sharedCapacityExpansionMax": None,
                         "sharedCapacityExpansionMin": None},
                        system_with_ee_restriction=True)
        else:
            print("WARNING: Shared Expansion Limitation for Renewable Energies will overrule the individual expansion limitation per region. CapacityFix, capacityMin and capacityMax is set to None.")
            if self.parent.targetyearoptimization:
                expansion_interval = self.parent.targetyear-self.parent.refyear
                # identify how much GW will be decomissioned, as this needs to be compensated
                pathway_decommissioning = pd.Series()
                temp_stock_decommission = pd.read_excel(
                    os.path.join(self.parent.temppath, 'temp_stock_decommission.xlsx'), index_col=0)
                for tech in ["onshore", "offshore", "openfield_pv", "rooftop_pv"]:
                    tech_items = [
                        x for x in self.parent.esM.componentNames if self.parent.string_identifiers[tech] in x and "_stock" not in x]
                    pathway_decommissioning[tech] =\
                        temp_stock_decommission[tech_items].sum().sum()
            else:
                self.getSharedExpansionCapacityMinRenewables()
                expansion_interval = self.parent.interval

            # for every technology, limit all techs
            for tech in ["onshore", "offshore", "openfield_pv", "rooftop_pv"]:
                tech_items = [
                    x for x in self.parent.esM.componentNames if self.parent.string_identifiers[tech] in x and "_stock" not in x]

                # get yearly limit
                if tech == "onshore":
                    yearly_expansion_limit = self.parent.maxYearlyExpansionGW_Onshore
                    _id = "OnshoreExpansion"
                elif tech == "offshore":
                    yearly_expansion_limit = self.parent.maxYearlyExpansionGW_Offshore
                    _id = "OffshoreExpansion"
                elif tech == "openfield_pv":
                    yearly_expansion_limit = self.parent.maxYearlyExpansionGW_OFPV
                    _id = "OpenfieldPVExpansion"
                elif tech == "rooftop_pv":
                    yearly_expansion_limit = self.parent.maxYearlyExpansionGW_RTPV
                    _id = "RooftopPVExpansion"
                else:
                    raise ValueError(
                        f"what is the expansion limit for? '{tech}'")

                for component in tech_items:
                    updated_parameter_dict = {}
                    # set up update dict of parameters
                    updated_parameter_dict["sharedExpansionID"] = _id
                    if self.parent.targetyearoptimization:
                        updated_parameter_dict["sharedCapacityExpansionMax"] = \
                            expansion_interval*yearly_expansion_limit \
                            - pathway_decommissioning[tech]
                    else:
                        updated_parameter_dict["sharedCapacityExpansionMax"] = \
                            expansion_interval*yearly_expansion_limit
                        exp_min =\
                            self.yearlyRenewableExpansionMin.loc[
                                (self.parent.currentyear-self.parent.interval+1): self.parent.currentyear,
                                tech].sum()
                        if exp_min == 0:
                            updated_parameter_dict["sharedCapacityExpansionMin"] = None
                            updated_parameter_dict["capacityMin"] = 0
                        else:
                            updated_parameter_dict["sharedCapacityExpansionMin"] = exp_min
                            updated_parameter_dict["capacityMin"] = 0

                        if component+"_stock" in self.parent.esM.componentNames:
                            updated_parameter_dict["capacityMax"] =  \
                                self.raw_ub[component] - \
                                self.parent.esM.getComponent(
                                    component+"_stock").capacityFix[self.parent.location]
                            if self.parent.currentyear == 2045:
                                if tech == "offshore":
                                    print("name")
                                    print(component)
                                    print(
                                        "updated_parameter_dict['capacityMax']")
                                    print(
                                        updated_parameter_dict["capacityMax"])
                                    print(
                                        "self.parent.esM.getComponent(component+'_stock').capacityFix[self.parent.location]")
                                    print(
                                        self.parent.esM.getComponent(
                                            component+"_stock").capacityFix[self.parent.location])

                        else:
                            updated_parameter_dict["capacityMax"] = self.raw_ub[component]
                            raise ValueError(
                                "Correct that region has no stock in 2025?")
                        updated_parameter_dict["capacityFix"] = None
                        self.helper_capex_calculation[
                            self.parent.currentyear].loc[
                            component, "modelyear_ub"] =\
                            updated_parameter_dict["sharedCapacityExpansionMax"]

                    # update component
                    if "sharedExpansionMin" in updated_parameter_dict.keys():
                        if updated_parameter_dict["sharedExpansionMin"] > updated_parameter_dict["sharedExpansionMax"]:
                            raise ValueError(
                                "sharedexpansionmin > sharedexpansionmax")
                    self.parent.esM = update_parameter(
                        self.parent.esM.getComponent(component),
                        self.parent.esM, updated_parameter_dict,
                        system_with_ee_restriction=True)

    def getOptimalCapacities(self):
        if self.parent.targetyearoptimization:
            year = "TargetYear"
        else:
            year = self.parent.currentyear

        self.resultCapacities[self.parent.currentyear] = pd.Series()
        for component_name in self.parent.esM.componentNames:
            component = self.parent.esM.getComponent(component_name)
            mdl_result_dict = self.parent.esM.componentModelingDict[component.modelingClass.__name__].getOptimalValues(
                name='capacityVariablesOptimum')['values']
            if component_name in mdl_result_dict.index:
                self.resultCapacities[year][component_name] = mdl_result_dict.loc[component_name][0]
            elif component.capacityMax is None and component.capacityMin is None:
                if component.capacityFix is not None:
                    self.resultCapacities[year][component_name] = component.capacityFix[0]
                elif "CO2Environment" in component_name or "Virt" in component_name:
                    continue
                else:
                    print("component")
                    print(component.__dict__)
                    raise ValueError()
            else:
                print("\n debug")
                print(component.name)
                print("component.capacityFix")
                print(component.capacityFix)
                print("component.capacityMax")
                print(component.capacityMax)
                print("component.capacityMin")
                print(component.capacityMin)

    def updateGHGlimits(self):
        self.parent.esM.add(
            fn.Sink(
                esM=self.parent.esM, name='CO2Environment',
                commodity='CO2Out', commodityLimitID='CO2_cap',
                hasCapacityVariable=False,
                yearlyLimit=self.parent.GHGlimit[self.parent.currentyear],
                interestRate=self.parent.default_wacc))

    def round_components_in_network(self):
        for component_name, mdl in self.parent.esM.componentNames.items():
            component = self.parent.esM.getComponent(component_name)
            if component.capacityMax is not None and component.capacityMin is not None:
                component.capacityMax[self.parent.location] = _round(
                    component.capacityMax[self.parent.location])
                component.capacityMin[self.parent.location] = _round(
                    component.capacityMin[self.parent.location])

            if not self.parent.targetyearoptimization and (self.parent.refyear != self.parent.currentyear):
                try:
                    self.parent.esM.componentModelingDict[component.modelingClass.__name__].getOptimalValues(name='capacityVariablesOptimum')['values'].loc[component.name, 'GermanyRegion'] = _round(
                        self.parent.esM.componentModelingDict[component.modelingClass.__name__].getOptimalValues(name='capacityVariablesOptimum')['values'].loc[component.name, 'GermanyRegion'])
                except:
                    # BEV int storage without result capacity in 2020
                    pass

    def initialize_temp_files(self):
        # list of all stock nodes
        listNotStock = [x for x in self.parent.esM.componentNames.keys()
                        if "_stock" not in x]

        # create temp_stock_decomission and compensation
        years = range(self.parent.refyear, self.parent.targetyear + 1)
        temp_stock_decommission = pd.DataFrame(
            index=years, columns=listNotStock, data=0)
        temp_stock_compensation = pd.DataFrame(
            index=years, columns=listNotStock, data=0)
        temp_stock_commission = pd.DataFrame(
            index=years, columns=listNotStock, data=0)
        historical_interval = range(
            self.parent.refyear - self.parent.interval,
            self.parent.targetyear + 1)
        temp_stock_capacity = pd.DataFrame(
            index=historical_interval, columns=listNotStock, data=0)

        for component in listNotStock:
            # 1. if there is forced decomissioning
            if component in self.parent.forceddecommissioning.columns:
                # 1.1 capacities
                temp_stock_capacity.loc[
                    historical_interval, component] = \
                    self.parent.forceddecommissioning.loc[
                    historical_interval, component]
                # 1.2. commissioning
                temp_stock_commission.loc[self.parent.refyear, component] += \
                    self.parent.historicalcapacity.loc[
                        self.parent.refyear, component]
                # 1.3. decomissioning
                temp_stock_decommission[component] = \
                    self.parent.forceddecommissioning[component].diff(
                        periods=-1).loc[years]
                temp_stock_decommission[component] = abs(
                    temp_stock_decommission[component])

            # 2. if component has historical data
            elif component in self.parent.historicalcapacity.columns:
                # 2.1 capaciies
                temp_stock_capacity.loc[historical_interval, component] += \
                    self.parent.historicalcapacity.loc[
                    historical_interval, component]
                # 2.2 comissioning
                temp_stock_commission.loc[self.parent.refyear, component] += \
                    self.parent.historicalcapacity.loc[
                    self.parent.refyear, component]
                # 2.3 decomissioning
                temp_stock_decommission[component] = \
                    self.parent.historicalcapacity[component].diff(
                        periods=-1).loc[years]
                # make decomissioning positive
                temp_stock_decommission[component] = abs(
                    temp_stock_decommission[component])

        # round the files
        temp_stock_commission = _round(temp_stock_commission)
        temp_stock_decommission = _round(temp_stock_decommission)
        temp_stock_compensation = _round(temp_stock_compensation)
        temp_stock_capacity = _round(temp_stock_capacity)

        # save the temp files
        temp_stock_commission.to_excel(
            os.path.join(self.parent.temppath, 'temp_stock_commission.xlsx'))
        temp_stock_decommission.to_excel(
            os.path.join(self.parent.temppath, 'temp_stock_decommission.xlsx'))
        temp_stock_compensation.to_excel(
            os.path.join(self.parent.temppath, 'temp_stock_compensation.xlsx'))
        temp_stock_capacity.to_excel(
            os.path.join(self.parent.temppath, 'temp_stock_capacity.xlsx'))
        return

    def createNewStockInNetwork(self):
        """Create empty stock nodes for transformation path analysis."""
        # Create stocks for components from first optimization step after start
        # year until target year (initial target year optimization excluded!)
        if self.parent.currentyear > self.parent.startyear:
            # node lists
            listNodes = [x for x in self.parent.esM.componentNames.keys()]
            list_restricted_nodes = listNodes.copy()
            list_new_stock_components = []

            # add stock nodes for components
            for i_name in listNodes:
                # get component
                i = self.parent.esM.getComponent(i_name)
                # pass if component without stock restriction
                if i_name in self.unrestricted_components or i_name == "CO2Environment" or "Virt" in i_name:
                    list_restricted_nodes.remove(i_name)
                    continue
                # new capacity build?
                result = self.resultCapacities[self.parent.currentyear][i.name]
                if result > 0.01:
                    if '_stock' in i.name:
                        pass
                    elif (i.name + '_stock') in list_restricted_nodes:
                        pass  # not add new stock if stock comp exists
                    else:  # add new stock components
                        list_new_stock_components.append(i.name)
                        # 1. Sources and Sinks
                        if isinstance(i, fn.Source):
                            self.parent.esM = addSource(
                                self.parent.esM,
                                sourceParameter={
                                    "name": i.name+"_stock",
                                    "commodity": i.commodity,
                                    "hasCapacityVariable": i.hasCapacityVariable,
                                    "operationRateMax": i.operationRateMax,
                                    "operationRateFix": i.operationRateFix,
                                    "capacityFix": i.capacityFix,
                                    "capacityMax": i.capacityMax,
                                    "capacityMin": i.capacityMin,
                                    "investPerCapacity": i.investPerCapacity,
                                    "opexPerCapacity": i.opexPerCapacity,
                                    "opexPerOperation": i.opexPerOperation,
                                    "commodityCost": i.commodityCost,
                                    "commodityRevenue": i.commodityRevenue,
                                    "interestRate": i.interestRate,
                                    "economicLifetime": i.economicLifetime,
                                    "technicalLifetime": i.technicalLifetime,
                                    "sharedCapacityExpansionMin": None,
                                    "sharedCapacityExpansionMax": None,
                                    "sharedExpansionID": None,
                                    "QPcostScale": None},
                                system_with_ee_restriction=self.parent.considerMaxYearlyExpansionLimit)

                        elif isinstance(i, fn.Sink):
                            self.parent.esM = addSink(
                                self.parent.esM,
                                sinkParameter={
                                    "name": i.name+"_stock",
                                    "commodity": i.commodity,
                                    "hasCapacityVariable": i.hasCapacityVariable,
                                    "operationRateMax": i.operationRateMax,
                                    "operationRateFix": i.operationRateFix,
                                    "capacityFix": i.capacityFix,
                                    "capacityMax": i.capacityMax,
                                    "capacityMin": i.capacityMin,
                                    "investPerCapacity": i.investPerCapacity,
                                    "opexPerCapacity": i.opexPerCapacity,
                                    "opexPerOperation": i.opexPerOperation,
                                    "commodityCost": i.commodityCost,
                                    "commodityRevenue": i.commodityRevenue,
                                    "interestRate": i.interestRate,
                                    "economicLifetime": i.economicLifetime,
                                    "technicalLifetime": i.technicalLifetime,
                                    "QPcostScale": None})
                        # 2. Transformers
                        elif isinstance(i, fn.Conversion):
                            self.parent.esM = addConversion(
                                esM=self.parent.esM,
                                conversionParameter={
                                    "name": i.name + '_stock',
                                    "physicalUnit": i.physicalUnit,
                                    "hasCapacityVariable": i.hasCapacityVariable,
                                    "commodityConversionFactors": i.commodityConversionFactors,
                                    "capacityMin": None,
                                    "capacityMax": None,
                                    "capacityFix": 0,
                                    "operationRateFix": i.operationRateFix,
                                    "opexPerOperation": i.opexPerOperation,
                                    "opexPerCapacity": i.opexPerCapacity,
                                    "investPerCapacity": i.investPerCapacity,
                                    "technicalLifetime": i.technicalLifetime,
                                    "economicLifetime": i.economicLifetime,
                                    "interestRate": i.interestRate,
                                    "QPcostScale": None})

                        # 3. Storages
                        elif isinstance(i, fn.Storage):
                            self.parent.esM = addStorage(
                                esM=self.parent.esM,
                                storageParameter={
                                    "name": i.name + '_stock',
                                    "commodity": i.commodity,
                                    "hasCapacityVariable": i.hasCapacityVariable,
                                    "capacityMin": None,
                                    "capacityMax": None,
                                    "capacityFix": 0,
                                    "investPerCapacity": i.investPerCapacity,
                                    "opexPerCapacity": i.opexPerCapacity,
                                    "opexPerChargeOperation": i.opexPerChargeOperation,
                                    "technicalLifetime": i.technicalLifetime,
                                    "economicLifetime": i.economicLifetime,
                                    "interestRate": i.interestRate,
                                    "chargeEfficiency": i.chargeEfficiency,
                                    "dischargeEfficiency": i.dischargeEfficiency,
                                    "selfDischarge": i.selfDischarge}
                            )

                        else:
                            raise ValueError(
                                "Unknown component type for "+i.name)
        return

    def calculateStockCapacities(self):
        """Calculate stocks for transformation path analysis."""
        # list of all stock nodes
        listStock = [x for x in self.parent.esM.componentNames
                     if "_stock" in x]
        # 0. temp files for stock decomissioning and compensation
        temp_stock_commission = pd.read_excel(
            os.path.join(self.parent.temppath, 'temp_stock_commission.xlsx'),
            index_col=0)
        temp_stock_decommission = pd.read_excel(
            os.path.join(self.parent.temppath, 'temp_stock_decommission.xlsx'),
            index_col=0)
        temp_stock_compensation = pd.read_excel(
            os.path.join(self.parent.temppath, 'temp_stock_compensation.xlsx'),
            index_col=0)
        temp_stock_capacity = pd.read_excel(
            os.path.join(self.parent.temppath, 'temp_stock_capacity.xlsx'),
            index_col=0)

        for stock_component_name in listStock:
            not_stock_name = stock_component_name.replace("_stock", "")
            # 1. add stock components to temp file if not existing yet
            if not_stock_name not in temp_stock_decommission.columns:
                temp_stock_decommission[not_stock_name] = 0
            if not_stock_name not in temp_stock_compensation.columns:
                temp_stock_compensation[not_stock_name] = 0
            if not_stock_name not in temp_stock_commission:
                temp_stock_commission[not_stock_name] = 0

            # 2. Save result of previous year to stock and save previous
            # capacity
            stock_component = self.parent.esM.getComponent(
                stock_component_name)
            component = self.parent.esM.getComponent(not_stock_name)

            # 2b) Save commissioning data to temp-files
            if self.parent.currentyear == self.parent.refyear:
                pass
            elif self.parent.currentyear == self.parent.startyear:
                pass
            else:
                result = self.resultCapacities[self.parent.currentyear][component.name]
                # self.parent.esM.componentModelingDict[component.modelingClass.__name__].getOptimalValues(
                #            name='capacityVariablesOptimum')['values'].loc[component.name, 'GermanyRegion']
                temp_stock_commission.loc[
                    self.parent.currentyear - self.parent.interval, not_stock_name] = \
                    result
                temp_stock_capacity.loc[
                    (self.parent.currentyear - self.parent.interval):(self.parent.currentyear - self.parent.interval + component.technicalLifetime[0]-1),
                    not_stock_name] += result

        # 2. Target Year or reference year
        if self.parent.targetyearoptimization or (self.parent.currentyear <= self.parent.startyear):
            for stock_component_name in listStock:
                # 2.1 Historical Data
                not_stock_name = stock_component_name.replace("_stock", "")
                if not_stock_name in self.parent.historicalcapacity.columns:
                    stock_component = self.parent.esM.getComponent(
                        stock_component_name)
                    component = self.parent.esM.getComponent(not_stock_name)

                    # set stock lb and ub to remaining historical capacity
                    # in targetyear
                    historical_rest_capacity = self.parent.historicalcapacity.loc[
                        self.parent.currentyear, not_stock_name]
                    self.parent.esM = update_parameter(
                        component=stock_component,
                        esM=self.parent.esM,
                        updated_parameter_dict={
                            "capacityMin": None,
                            "capacityMax": None,
                            "capacityFix": _round(float(historical_rest_capacity))})
                    if self.parent.targetyearoptimization:
                        if component.capacityMax is None:
                            _capacity = component.capacityFix[self.parent.location]
                        else:
                            _capacity = component.capacityMax[self.parent.location]
                        # 1. forced decomission
                        if component.name in self.parent.forceddecommissioning.columns:
                            forced_value = self.parent.forceddecommissioning.loc[
                                self.parent.currentyear, component.name]
                            self.parent.esM = update_parameter(
                                component=component,
                                esM=self.parent.esM,
                                updated_parameter_dict={
                                    "capacityMin": None,
                                    "capacityMax": None,
                                    "capacityFix": _round(float(forced_value))})
                        # 2. historical
                        elif _capacity < historical_rest_capacity:
                            raise ValueError(
                                "Component {}".format(component.name) +
                                " has historical rest capacity of " +
                                "'{}'".format(historical_rest_capacity) +
                                " GW in target year, but ub in parameter " +
                                "file is only '{}'".format(_capacity))
                        else:
                            new_capacitymax = self.raw_ub[component.name] - \
                                historical_rest_capacity
                            if component.capacityMax is not None:
                                self.parent.esM = update_parameter(
                                    component=component,
                                    esM=self.parent.esM,
                                    updated_parameter_dict={
                                        "capacityMax": new_capacitymax,
                                        "capacityMin": component.capacityMin,
                                        "capacityFix": component.capacityFix})
                            else:
                                self.parent.esM = update_parameter(
                                    component=component,
                                    esM=self.parent.esM,
                                    updated_parameter_dict={
                                        "capacityMin": None,
                                        "capacityMax": None,
                                        "capacityFix": new_capacitymax})
                            # check
                            component = self.parent.esM.getComponent(
                                component.name)
                            if component.capacityMax is not None:
                                if component.capacityMax[self.parent.location] < component.capacityMin[self.parent.location]:
                                    raise ValueError(
                                        "UB is greater than lb for " +
                                        "{}".format(component.name))

            if not self.parent.targetyearoptimization:  # refyear
                for component_name in self.parent.esM.componentNames:
                    component = self.parent.esM.getComponent(component_name)
                    if component_name in self.unrestricted_components:
                        continue
                    elif "Virt" in component_name:
                        continue
                    elif "CO2Environment" in component_name:
                        continue
                    if "_stock" not in component_name:
                        self.parent.esM = update_parameter(
                            component=component,
                            esM=self.parent.esM,
                            updated_parameter_dict={
                                "capacityMin": None,
                                "capacityMax": None,
                                "capacityFix": 0.0})

        # 3. Optimization Years
        else:
            for stock_component_name in listStock:
                component_name = stock_component_name.replace("_stock", "")
                stock_component = self.parent.esM.getComponent(
                    stock_component_name)
                component = self.parent.esM.getComponent(component_name)

                # 3.0 add capacity of component to lb and ub of stock-value
                entire_capacity = temp_stock_capacity.loc[
                    self.parent.currentyear, component_name]
                self.parent.esM = update_parameter(
                    component=stock_component,
                    esM=self.parent.esM,
                    updated_parameter_dict={
                        "capacityMin": None,
                        "capacityMax": None,
                        "capacityFix": _round(entire_capacity)})
                stock_component = self.parent.esM.getComponent(
                    stock_component_name)
                if stock_component.capacityFix[self.parent.location] < 0:
                    raise ValueError(
                        "Negative lower bound for stock " +
                        "'{}': {}".format(
                            stock_component.name, stock_component.capacityMin[self.parent.location]))

                # 3.2 Compensating stock
                decommission_year = int(self.parent.currentyear - self.parent.interval +
                                        component.technicalLifetime[0])
                if decommission_year <= self.parent.targetyear:
                    # write capacity to decommission in temp
                    result = self.resultCapacities[self.parent.currentyear][component.name]
                    temp_stock_decommission.loc[decommission_year,
                                                component.name] += result

                # 4.4. Compensate future stock removal
                # if decomissioning is happening during pathway
                if decommission_year <= self.parent.targetyear:
                    t = 0
                    # iterate over previous interval from decommiss.year
                    for year in np.linspace(
                            self.parent.currentyear - self.parent.interval +
                        component.technicalLifetime[0] - 1,
                            self.parent.currentyear +
                        component.technicalLifetime[0] -
                            2 * self.parent.interval + 1,
                            self.parent.interval - 1):
                        year = int(year)
                        t += 1
                        # if there is temp_stock
                        if temp_stock_compensation.loc[year, component.name] >= 0:
                            if year == decommission_year - 1:
                                temp_stock_compensation.loc[year, component.name] += (
                                    (temp_stock_decommission.loc[year + 1, component.name]) / (component.technicalLifetime[0] - 2)) * 2
                            # increase temp_stock from component for
                            # previous interval of decommission year
                            else:
                                temp_stock_compensation.loc[year, component.name] += (
                                    temp_stock_compensation.loc[year + 1, component.name] * (
                                        (component.technicalLifetime[0] - 2 - t) / (component.technicalLifetime[0] - 2)))

        # round the files and lb and ub
        temp_stock_decommission = _round(temp_stock_decommission)
        temp_stock_compensation = _round(temp_stock_compensation)
        temp_stock_commission = _round(temp_stock_commission)
        temp_stock_capacity = _round(temp_stock_capacity)
        for component_name in self.parent.esM.componentNames:
            component = self.parent.esM.getComponent(component_name)
            if component.capacityMax is not None:
                component.capacityMax[self.parent.location] = \
                    _round(component.capacityMax[self.parent.location])
                component.capacityMin[self.parent.location] = \
                    _round(component.capacityMin[self.parent.location])

        # Export yearly temp files
        temp_stock_decommission.to_excel(
            os.path.join(self.parent.temppath, 'temp_stock_decommission.xlsx'))
        temp_stock_compensation.to_excel(
            os.path.join(self.parent.temppath, 'temp_stock_compensation.xlsx'))
        temp_stock_commission.to_excel(
            os.path.join(self.parent.temppath, 'temp_stock_commission.xlsx'))
        temp_stock_capacity.to_excel(
            os.path.join(self.parent.temppath, 'temp_stock_capacity.xlsx'))

        # Final check
        self.check_if_lb_greater_ub()
        self.fixNumericalInstabilities()
        return

    def calculateNewTechs(self):
        """Calculate new technology parameter for transformation path"""
        listAllNodes = [x for x in self.parent.esM.componentNames]
        listStock = [x for x in self.parent.esM.componentNames
                     if "_stock" in x]

        sCurve_lb = pd.read_excel(
            os.path.join(self.parent.temppath, 'temp_scurve_lb.xlsx'), index_col=0)
        sCurve_ub = pd.read_excel(
            os.path.join(self.parent.temppath, 'temp_scurve_ub.xlsx'), index_col=0)
        temp_stock_compensation = pd.read_excel(
            os.path.join(self.parent.temppath, 'temp_stock_compensation.xlsx'), index_col=0)

        ######################################################################
        def get_expansion_funnel_for_interval(self, component):
            # 0. pass for reference year -> no expansion
            if self.parent.currentyear == self.parent.refyear:
                expansion_min = 0
                expansion_max = 0
            # 0. pass for components with raw_ub = 0 -> only decomissioning
            if self.raw_ub[component.name] == 0:
                expansion_min = 0
                expansion_max = 0
            else:
                # 1.  get expansion of last interval
                # 1a) for start year and component with historical capacity
                if self.parent.currentyear == self.parent.startyear and component.name in self.parent.historicalcapacity:
                    # component with historical capacity
                    # get historical data
                    hist_cap_refyear = self.parent.historicalcapacity.loc[
                        self.parent.refyear, component.name]
                    hist_cap_refyearminusinterval = self.parent.historicalcapacity.loc[
                        self.parent.refyear - 5, component.name]
                    hist_cap_currentyear = self.parent.historicalcapacity.loc[
                        self.parent.currentyear, component.name]
                    # increasing technology
                    if (_round(hist_cap_refyear) > _round(hist_cap_refyearminusinterval)):
                        increasing_tech = True
                        yearly_expansion_previous_interval = \
                            (hist_cap_refyear - hist_cap_refyearminusinterval) \
                            / self.parent.interval
                    # decreasing technology
                    else:
                        increasing_tech = False
                        yearly_expansion_previous_interval = \
                            hist_cap_currentyear / \
                            component.technicalLifetime[0]
                # 1b) for start year without historical capacity or
                # for further years based on installed capacity
                else:
                    result = self.resultCapacities[self.parent.currentyear][component.name]
                    yearly_expansion_previous_interval = result / self.parent.interval

                    # diffusion value for initial capacity increase
                    start_diffusion_capacity_increase = self.parent.startDiffusion * \
                        self.raw_ub[component.name]

                    if yearly_expansion_previous_interval < start_diffusion_capacity_increase:
                        yearly_expansion_previous_interval = \
                            start_diffusion_capacity_increase
                        increasing_tech = False
                    else:
                        increasing_tech = True

                if yearly_expansion_previous_interval < 0:
                    raise ValueError(
                        "Problem with yearly_expansion_previous interval for" +
                        " '{}'".format(component.name))

                # 2) calculate expansion_max and min by previous capacity
                # increase
                expansion_min = 0
                expansion_max = 0
                for a in range(1, self.parent.interval + 1):
                    expansion_max += yearly_expansion_previous_interval * \
                        (1 + self.parent.MaxExpansion)**a
                    if increasing_tech:
                        expansion_min += yearly_expansion_previous_interval * \
                            (1 - self.parent.MaxDecommissioning)**a
                    else:
                        expansion_min += 0
            return expansion_min, expansion_max

        def _buildings_ub_lb(self, component, expansion_max, expansion_min):
            # TODO NEW MASTERTHESIS FOR BUILDING SECTOR
            # Check for new buildings which have stock since last update
            # 2.5% Sanierungsrate basierend auf 40 Jahren techn. Lebensdauer
            # 2.5% Sanierungsrate basierend auf 40 Jahren techn.
            # Lebensdauer
            refurbCorFactor = (self.parent.maxRefurbishmentRate)/100

            if any(z in component.name for z in self.parent.string_identifiers["building_categories"]):
                if any(zz in component.name for zz in self.parent.string_identifiers["building_exclusion_list"]):
                    pass
                elif self.parent.currentyear == self.parent.refyear:
                    pass
                else:
                    # Reference buildings
                    if component.name in self.parent.historicalcapacity:
                        expansion_min = 0
                        # only compensate for removal of historical
                        # capacity
                        expansion_max = (self.parent.historicalcapacity.loc[
                            self.parent.currentyear - self.parent.interval, component.name] -
                            self.parent.historicalcapacity.loc[
                            self.parent.currentyear, component.name])

                    # Sanierungspakete
                    # TODO used shared potential um ein Gebäude mit mehreren
                    # Sanierungspaketen refurbishen zu können
                    else:
                        # check which refurbishment package is used and get
                        # name of reference building in historical data
                        temp_list = [
                            component.name.replace(x, "") for x in self.parent.string_identifiers["refurbishmentpackages"] if component.name.endswith(x)]
                        if len(temp_list) != 1:
                            raise ValueError(
                                "Unclear mapping for component " +
                                "'{}'. ".format(component.name) +
                                "List of historical buildings " +
                                "'{}'".format(temp_list) +
                                "Add refurbishment package to list in json " +
                                "or double check.")
                        # TODO elif! -> Neubau fehlt! -K40 und -K55
                        else:
                            historical_name = temp_list[0]
                            # TODO verbessern
                            _addition_max = 0
                            for year in range(self.parent.currentyear-self.parent.interval+1, self.parent.currentyear+1):
                                value = (
                                    self.parent.historicalcapacity[historical_name][year] - _addition_max) * refurbCorFactor
                                if value > 0:
                                    _addition_max += value
                            expansion_max = _addition_max
                            expansion_min = 0
            return expansion_min, expansion_max

        def _compensating_future_removal(self, component, expansion_min, expansion_max, temp_stock_compensation):
            if component.name in temp_stock_compensation.index:
                # INCREASE LOWER BOUND IF TECHNOLOGY IS AN INCREASING TECH
                # is capacity of component expanded
                # lb only increased if technology is insalled,
                # no compensation for "auslaufende" technologies
                if sCurve_lb.loc[self.parent.targetyear, component.name] > sCurve_lb.loc[self.parent.refyear, component.name]:
                    if temp_stock_compensation[component.name][self.parent.currentyear] > 0:
                        expansion_min += \
                            temp_stock_compensation.loc[
                                self.parent.currentyear - self.parent.interval:self.parent.currentyear, component.name].sum()
                        if expansion_min > self.raw_ub[component.name]:
                            expansion_min = self.raw_ub[component.name]

                # INCREASE UPPER BOUND
                if temp_stock_compensation[component.name][self.parent.currentyear] > 0:
                    expansion_max +=\
                        temp_stock_compensation.loc[
                            self.parent.currentyear - self.parent.interval:self.parent.currentyear, component.name].sum()
                    if expansion_max > self.raw_ub[component.name]:
                        expansion_max = self.raw_ub[component.name]

            return expansion_min, expansion_max

        ######################################################################
        # 2. Write upper bound and lower bound of component for year
        for component_name in self.parent.esM.componentNames:
            component = self.parent.esM.getComponent(component_name)
            # stock capacity of year:
            if component.name + "_stock" in listStock:
                stock_capacity = self.parent.esM.getComponent(
                    component.name + "_stock").capacityFix[self.parent.location]
            else:
                stock_capacity = 0
            # 2a) Stock components
            if "_stock" in component.name:
                continue

            if "Virt" in component.name or "CO2Environment" in component.name:
                continue

            # 2b) Unrestricted Components
            if component.name in self.unrestricted_components:
                if component.name + "_stock" in listAllNodes:
                    stock_cap = self.parent.esM.getComponent(
                        component.name + "_stock").capacityFix[self.parent.location]
                else:
                    stock_cap = 0

                if stock_cap < sCurve_lb.loc[
                        self.parent.currentyear, component.name]:
                    new_min_capacity = sCurve_lb.loc[
                        self.parent.currentyear, component.name] - stock_cap
                else:
                    new_min_capacity = 0
                new_max_capacity = sCurve_ub.loc[
                    self.parent.currentyear, component.name] - stock_cap
                if new_min_capacity == new_max_capacity:
                    self.parent.esM = update_parameter(
                        component=component,
                        esM=self.parent.esM,
                        updated_parameter_dict={
                            "capacityMin": None,
                            "capacityMax": None,
                            "capacityFix": new_max_capacity})
                else:
                    self.parent.esM = update_parameter(
                        component=component,
                        esM=self.parent.esM,
                        updated_parameter_dict={
                            "capacityMin": new_min_capacity,
                            "capacityMax": new_max_capacity,
                            "capacityFix": None})
                continue

            # 2c) Restricted components
            # 2.c.1) Calculated expansion min and max based on allowed
            # expansion
            expansion_min, expansion_max = get_expansion_funnel_for_interval(
                self, component)

            expansion_max = _round(expansion_max)
            expansion_min = _round(expansion_min)

            # 2.c.2) Check if expansion_min and _max are within the borders of
            # sCurves, otherwise limit to sCurves
            sCurve_lb_value = sCurve_lb.loc[self.parent.currentyear,
                                            component.name]
            sCurve_ub_value = sCurve_ub.loc[self.parent.currentyear,
                                            component.name]

            # check if expansion_min and expansion_max in sCurve, otherwise
            # limit to sCurve interval
            if (stock_capacity + expansion_min) < sCurve_lb_value:
                expansion_min = sCurve_lb_value - stock_capacity
                if expansion_min < 0:
                    print("Warning: Negative expansion_min for "+component.name)
                    print("sCurve_lb_value : {}, stock_capacity: {}, resulting expansion_min: {}".format(
                        sCurve_lb_value, stock_capacity, expansion_min))
                if expansion_max < expansion_min:
                    print("Important: expansion_max ('{}') of '{}' is lower than sCurve_lb and therefore the upper bound for the next period is corrected to fit the sCurve_lb. However, this is contrary to the capacity increase funnel.".format(
                        expansion_max, component.name))
                    expansion_max = expansion_min

            if (stock_capacity + expansion_max) > sCurve_ub_value:
                expansion_max = sCurve_ub_value - stock_capacity
                if expansion_max < 0:
                    print("\nWarning: Negative expansion_max for "+component.name)
                    print("sCurve_ub_value : {}, stock_capacity: {}, resulting expansion_max: {}".format(
                        sCurve_ub_value, stock_capacity, expansion_max))

                if expansion_max < expansion_min:
                    print("Important: expansion_min ('{}') of '{}' is higher than sCurve_ub and therefore the lower bound for the next period is corrected to be lower than sCurve_ub. However, this is contrary to the capacity decrease funnel.".format(
                        expansion_min, component.name))
                    expansion_min = expansion_max

            # numeric instabilities
            expansion_max = _round(expansion_max)
            expansion_min = _round(expansion_min)
            _check_if_lb_greater_ub(
                lb=expansion_min, ub=expansion_max, name=component.name)
            # 2.c.3) Compensate future removal
            expansion_min, expansion_max = _compensating_future_removal(
                self, component, expansion_min, expansion_max,
                temp_stock_compensation)

            # lb/ub buildings
            expansion_min, expansion_max = _buildings_ub_lb(
                self, component=component, expansion_max=expansion_max,
                expansion_min=expansion_min)

            # 2.c.4) set the ub and lb
            if expansion_min == expansion_max:
                self.parent.esM = update_parameter(
                    component=component,
                    esM=self.parent.esM,
                    updated_parameter_dict={
                        "capacityMin": None,
                        "capacityMax": None,
                        "capacityFix": expansion_min})
            else:
                self.parent.esM = update_parameter(
                    component=component,
                    esM=self.parent.esM,
                    updated_parameter_dict={
                        "capacityMin": expansion_min,
                        "capacityMax": expansion_max,
                        "capacityFix": None})

        #######################################################################
        # 3. Transport Correction for all years
        # TODO Masterthesis
        # Iterate over all transport components
        # for transport_component in self.parent.transport.index:
        #     # slimit per transport category
        #     sLimit = 0

        #     # get nodes with transport component in name
        #     list_of_nodes_with_transport_component_in_name = [
        #         x for x in listAllNodes if (transport_component in x)]

        #     if self.parent.currentyear != self.parent.refyear:
        #         # iterate over relevant components
        #         for comp in list_of_nodes_with_transport_component_in_name:
        #             # check that component does not have part of exclusion list
        #             if not any(exclusion in comp for exclusion in self.unrestricted_components):
        #                 # if component has stock increase sLimit
        #                 if comp + "_stock" in listStock:
        #                     sLimit += self.parent.esM.getComponent(comp).capacityMin[self.parent.location]
        #                     print("increasing sLimit for" + comp)

        #     # WAS SOLL DIESE BERECHNUNG?
        #     if sLimit > self.parent.transport.loc[transport_component, "Netto"] * (self.parent.interval) * 0.9:
        #         for comp in list_of_nodes_with_transport_component_in_name:
        #             # check that component does not have part of exclusion list
        #             if any(exclusion in comp for exclusion in self.unrestricted_components):
        #                 pass
        #             elif comp + '_stock' in listStock:
        #                 if transport_component in comp:
        #                     component = self.parent.esM.getComponent(comp)
        #                     component.capacityMax[self.parent.location] = component.capacityMin[self.parent.location] * 1.5 + 2.5  # was lb*3+5

        #######################################################################
        # 3. BEV storage capacities
        if self.parent.string_identifiers["BatteryElectricCar"] in listAllNodes:
            if self.parent.currentyear >= self.parent.startyear and self.parent.currentyear < self.parent.targetyear:
                result = self.resultCapacities[self.parent.currentyear][
                    self.parent.string_identifiers["BatteryElectricCar"]]
                # self.parent.esM.componentModelingDict[component.modelingClass.__name__].getOptimalValues(
                #             name='capacityVariablesOptimum')['values'].loc[self.string_identifiers["BatteryElectricCar"], 'GermanyRegion']
                if result > 0.1:
                    value = self.parent.esM.getComponent(self.parent.string_identifiers["BatteryElectricCar"]+"_stock").capacityFix[self.parent.location] * \
                        self.parent.BEVstoragefactor
                    bev_component = self.parent.esM.getComponent(
                        self.parent.string_identifiers["BatteryElectricCar_Storage"])
                    self.parent.esM = update_parameter(
                        component=bev_component,
                        esM=self.parent.esM,
                        updated_parameter_dict={
                            "capacityMin": None,
                            "capacityMax": None,
                            "capacityFix": value})

        #######################################################################
        # 4. prevent numeric issues
        self.fixNumericalInstabilities()
        self.check_if_lb_greater_ub()

        #######################################################################
        # 5. Final check of ub and lb
        for component_name in self.parent.esM.componentNames:
            component = self.parent.esM.getComponent(component_name)
            if component.name in sCurve_ub.columns:
                sCurve_ub_value = sCurve_ub.loc[self.parent.currentyear,
                                                component.name]
                if component.capacityMax is None:
                    ub = component.capacityFix[self.parent.location]
                else:
                    ub = component.capacityMax[self.parent.location]
                if component.name + "_stock" in listStock:
                    ub += self.parent.esM.getComponent(
                        component.name + "_stock").capacityFix[self.parent.location]
                if _round(ub) > _round(sCurve_ub_value):
                    print("Warning: Error for Component '{}' in year '{}': sCurve-Ub {} but UB for year: {}".format(
                        component.name, self.parent.currentyear, sCurve_ub_value, _round(ub)))

    def updateStockParameters(self):
        # write parameters for stock components
        # for target year and reference year
        if self.parent.targetyearoptimization or (self.parent.currentyear < self.parent.startyear):
            for component_name in self.parent.esM.componentNames:
                component = self.parent.esM.getComponent(component_name)
                if '_stock' in component.name:
                    newCapex = self.parent.capex.loc[component.name,
                                                     self.parent.refyear]
                    newOpexPerCapacity = newCapex * \
                        self.raw_opexFix[component.name]
                    self.parent.esM = update_parameter(
                        component,
                        self.parent.esM,
                        {"investPerCapacity": newCapex,
                         "opexPerCapacity": newOpexPerCapacity})

        # for optimization years
        else:
            temp_stock_capacity = pd.read_excel(
                os.path.join(self.parent.temppath, 'temp_stock_capacity.xlsx'), index_col=0)
            temp_stock_commission = pd.read_excel(
                os.path.join(self.parent.temppath,  'temp_stock_commission.xlsx'), index_col=0)
            # filter for components which have a stock and are not a connector
            all_nodes = [x for x in self.parent.esM.componentNames]
            components_with_stocks_and_not_hubs = [
                x for x in all_nodes if x + "_stock" in all_nodes and ("-Hub-"not in x)]
            if len(components_with_stocks_and_not_hubs) == 0:
                raise ValueError("No stocks updated")
            for component_name in components_with_stocks_and_not_hubs:
                component = self.parent.esM.getComponent(component_name)
                stock_component = self.parent.esM.getComponent(
                    component_name + "_stock")
                result = self.resultCapacities[self.parent.currentyear][component_name]
                if result < 0.01:
                    continue

                # determine commissioning capacities per year
                # check the last intervals and distribute the installed
                # capacity
                total_capacity = temp_stock_commission.loc[self.parent.currentyear - self.parent.interval, component_name] + temp_stock_capacity.loc[
                    self.parent.currentyear - self.parent.interval, component_name]
                # no updating if nothing is installed
                if total_capacity == 0:
                    continue

                _rest_capacity = total_capacity
                years = range(self.parent.currentyear,
                              self.parent.refyear - 1, -self.parent.interval)
                active_cap_per_comissioning_year = pd.Series(index=years)
                for year in years:
                    if _rest_capacity >= temp_stock_commission.loc[year, component_name]:
                        active_cap_per_comissioning_year[year] = \
                            temp_stock_commission.loc[year, component_name]
                        _rest_capacity -= \
                            temp_stock_commission.loc[year, component_name]
                    else:
                        active_cap_per_comissioning_year[
                            year] = _rest_capacity
                        break

                if active_cap_per_comissioning_year[self.parent.currentyear] != 0:
                    raise ValueError(
                        "Calculation of active capacity per commissioning year is wrong")

                relevant_years = active_cap_per_comissioning_year.index

                update_dict = {}
                # update co2 footprint -> currently only for transformers, as
                # sources and sinks have constant emissions
                if isinstance(component, fn.Conversion) and component.name not in self.parent.raw_heatpumps.index:
                    if self.parent.emissions.loc[component.name, relevant_years].sum() > 0:
                        _commodity_conversion = component.commodityConversionFactors
                        new_stock_emission = \
                            (active_cap_per_comissioning_year[
                                relevant_years] * self.parent.emissions.loc[component.name, relevant_years]).sum() / active_cap_per_comissioning_year.sum()
                        if new_stock_emission != 0:
                            _commodity_conversion.update(
                                {"CO2Out": new_stock_emission})
                        update_dict["commodityConversionFactors"] = _commodity_conversion

                # update efficiency
                if isinstance(component, fn.Conversion):
                    _efficiency_of_years = pd.Series(index=relevant_years)
                    _efficiency_dict = {}
                    for commod in component.commodityConversionFactors:
                        if commod == "CO2Out":
                            _efficiency_dict["CO2Out"] = component.commodityConversionFactors["CO2Out"]
                            continue
                        for year in relevant_years:
                            _efficiency_of_years[year] = self.parent.efficiency[component.name][year][commod]
                        _efficiency =\
                            ((_efficiency_of_years * active_cap_per_comissioning_year[relevant_years]).sum()) \
                            / (active_cap_per_comissioning_year[relevant_years].sum())
                        _efficiency_dict[commod] = _efficiency

                # update capex and costscale
                if isinstance(component, fn.Conversion) or isinstance(component, fn.Source) or isinstance(component, fn.Sink) or isinstance(component, fn.Storage):
                    # filter for time-horizon of lifetime -> exclude components
                    # between economic lifetime and technical lifetime
                    _relevant_year_capex = \
                        [x for x in relevant_years if
                            ((self.parent.currentyear - x) <
                             component.economicLifetime[self.parent.location])
                            and (x < self.parent.currentyear)
                            and (active_cap_per_comissioning_year[x] > 0)]
                    _relevant_year_capex.sort()

                    _cost_scale_corrected_total_capex = pd.Series(
                        index=_relevant_year_capex)

                    # per year
                    for year in _relevant_year_capex:
                        _capacity = active_cap_per_comissioning_year[year]

                        # for reference year, take the capex of parameter file
                        if year == self.parent.refyear:
                            capex_installed = \
                                self.parent.capex.loc[stock_component.name,
                                                      self.parent.refyear]
                        # for last year -
                        # need to calculate average capex for installed capacity
                        elif year == (self.parent.currentyear-self.parent.interval):
                            raw_capex = self.parent.capex.loc[component.name, year]
                            if self.parent.costscale.loc[component.name, year] != 0 and self.parent.qp is True:
                                def interpolation(d, x):
                                    return d[0][1] + (x - d[0][0]) * ((d[1][1] - d[0][1])/(d[1][0] - d[0][0]))

                                # datapoints
                                modelyear_ub = \
                                    self.helper_capex_calculation[year].loc[
                                        component.name, "modelyear_ub"]
                                costscale = \
                                    self.parent.costscale.loc[component.name, year]
                                datapoints = [
                                    [0, raw_capex*(1-costscale)],
                                    [modelyear_ub, raw_capex*(1+costscale)]]

                                # check if capex installed in range
                                if _capacity < 0 or _capacity > modelyear_ub:
                                    print("\nProblem with component")
                                    print(component.name)
                                    print(_capacity)
                                    print(modelyear_ub)
                                    raise ValueError(
                                        "Installed capacity is higher than range.")

                                # interpolate to max capex installed
                                capex_installed_max = interpolation(
                                    datapoints, _capacity)
                                capex_installed_min = raw_capex*(1-costscale)
                                capex_installed = capex_installed_min + \
                                    (capex_installed_max-capex_installed_min)/2

                                if _round(capex_installed) > _round(raw_capex) or _round(capex_installed_max) < 0:
                                    raise ValueError(
                                        "Calculated CAPEX for stock " +
                                        f"'{component.name}' is wrong." +
                                        f"Calculated value: {capex_installed}")

                            else:
                                capex_installed = raw_capex

                            self.helper_capex_calculation[year].loc[
                                component.name, "capex_av_installed"] = \
                                capex_installed
                        # for all years but last year (and refyear)
                        # apply the calculated costscale corrected average capex
                        else:
                            if year == self.parent.refyear:
                                capex_installed = \
                                    self.parent.capex.loc[stock_component.name,
                                                          self.parent.refyear]
                            else:
                                capex_installed = \
                                    self.helper_capex_calculation[year].loc[
                                        component.name, "capex_av_installed"]
                        # get the total capex of the model year
                        _cost_scale_corrected_total_capex[year] = \
                            _capacity * capex_installed

                    # get average capex over all model years
                    _recalculated_capex = \
                        _cost_scale_corrected_total_capex.sum() / \
                        active_cap_per_comissioning_year.sum()

                    update_dict["investPerCapacity"] = _recalculated_capex
                    update_dict["opexPerCapacity"] =\
                        _recalculated_capex*self.raw_opexFix[component.name]
                    if not isinstance(component, fn.Storage):
                        if self.parent.qp:
                            update_dict["QPcostScale"] = 0

                # stock fuel costs - no weighting but normal opex costs
                if stock_component.name.replace("_stock", "") in self.parent.fuelprices.index:
                    new_opex = self.parent.fuelprices.loc[
                        stock_component.name.replace("_stock", ""),
                        self.parent.currentyear]
                    update_dict["opexPerOperation"] = new_opex

                if update_dict:
                    self.parent.esM = update_parameter(
                        component,
                        self.parent.esM,
                        update_dict)

    def updateTechnologyParameters(self):
        self.helper_capex_calculation[self.parent.currentyear] = pd.DataFrame()
        listNotStock = [x for x in self.parent.esM.componentNames
                        if "_stock" not in x]

        # Give all not-stock components new parameters
        for component_name in listNotStock:
            i = self.parent.esM.getComponent(component_name)
            if (isinstance(i, fn.Source) or isinstance(i, fn.Sink)) and (i.name in self.unrestricted_components):
                continue
            if "Virt" in component_name or "CO2Environment" in component_name:
                continue
            if i.capacityMax is not None:
                self.helper_capex_calculation[self.parent.currentyear].loc[
                    i.name, "modelyear_ub"] = i.capacityMax[self.parent.location]
            else:
                self.helper_capex_calculation[self.parent.currentyear].loc[
                    i.name, "modelyear_ub"] = i.capacityFix[self.parent.location]
            # write new parameters depending on type of component
            if isinstance(i, fn.Source) or isinstance(i, fn.Sink):
                update_dict = {}
                new_capex = self.parent.capex.loc[i.name,
                                                  self.parent.currentyear]
                update_dict["investPerCapacity"] = new_capex
                if i.name in self.parent.raw_sources.index:
                    new_opexPerCapacity = self.raw_opexFix[i.name]*new_capex
                    update_dict["opexPerCapacity"] = new_opexPerCapacity
                else:
                    new_opexPerCapacity = self.raw_opexFix[i.name]*new_capex
                update_dict["opexPerCapacity"] = new_opexPerCapacity
                if self.parent.qp:
                    update_dict["QPcostScale"] =\
                        self.parent.costscale.loc[i.name,
                                                  self.parent.currentyear]
                else:
                    update_dict["QPcostScale"] = None
                if i.name in self.parent.fuelprices.index:
                    update_dict["opexPerOperation"] = \
                        self.parent.fuelprices.loc[i.name,
                                                   self.parent.currentyear]
                self.parent.esM = update_parameter(
                    i, self.parent.esM, update_dict)

            elif isinstance(i, fn.Conversion):
                new_capex = \
                    self.parent.capex.loc[i.name, self.parent.currentyear]
                if i.name in self.parent.raw_transformers.index:
                    new_opexPerCapacity = self.raw_opexFix[i.name]*new_capex
                else:
                    new_opexPerCapacity = self.raw_opexFix[i.name]*new_capex

                # heatpunps without costscale
                if self.parent.qp:
                    new_costscale = self.parent.costscale.loc[
                        i.name, self.parent.currentyear]
                else:
                    new_costscale = 0
                # new efficiency and emissions
                if i.name not in self.parent.raw_heatpumps.index:
                    new_emissions = \
                        self.parent.emissions.loc[i.name,
                                                  self.parent.currentyear]
                    new_commodity_conversion = \
                        self.parent.efficiency[i.name][self.parent.currentyear]
                    if new_emissions != 0:
                        new_commodity_conversion.update(
                            {"CO2Out": new_emissions})
                if i.name not in self.parent.raw_heatpumps.index:
                    self.parent.esM = update_parameter(
                        i, self.parent.esM,
                        {"investPerCapacity": new_capex,
                         "opexPerCapacity": new_opexPerCapacity,
                         "QPcostScale": new_costscale,
                         "commodityConversionFactors": new_commodity_conversion})
                else:
                    self.parent.esM = update_parameter(
                        i, self.parent.esM,
                        {"investPerCapacity": new_capex,
                         "opexPerCapacity": new_opexPerCapacity,
                         "QPcostScale": new_costscale})
            elif isinstance(i, fn.Storage):
                new_capex = \
                    self.parent.capex.loc[i.name, self.parent.currentyear]
                new_opexPerCapacity = self.raw_opexFix[i.name]*new_capex
                # new_costscale = \
                #     self.parent.costscale.loc[i.name, self.parent.currentyear]
                self.parent.esM = update_parameter(
                    i, self.parent.esM,
                    {"investPerCapacity": new_capex,
                     "opexPerCapacity": new_opexPerCapacity,
                     # "QPcostScale": new_costscale
                     })

    def calculateDemand(self):
        """Calculate Demands for transformation path analysis."""
        # buildings & industry
        for demand_sink_name in self.parent.esM.componentNames:
            demand_sink = self.parent.esM.getComponent(demand_sink_name)
            if isinstance(demand_sink, fn.Source) or isinstance(demand_sink, fn.Sink):
                if self.parent.string_identifiers['TSource'] in demand_sink.name or self.parent.string_identifiers['TSink'] in demand_sink.name:
                    pass
                elif self.parent.string_identifiers["AEDemand"] in demand_sink.name:
                    if demand_sink.name == self.parent.string_identifiers["LightingN-AEDemand"]:
                        pass
                    else:
                        self.parent.esM = update_parameter(
                            component=demand_sink,
                            esM=self.parent.esM,
                            updated_parameter_dict={
                                "capacityMin": None,
                                "capacityMax": None,
                                "capacityFix": self.parent.demand.loc[
                                    "HH-Electronics", self.parent.currentyear]})
                # industry
                elif self.parent.string_identifiers["IndustryDemand"] in demand_sink.name or self.parent.string_identifiers["CO2Sinks"] in demand_sink.name:
                    if demand_sink.name in self.parent.demand.index:
                        temp_dem = self.parent.demand.loc[
                            demand_sink.name, self.parent.currentyear] / 8.76
                        # 8.76 is due to hours
                        self.parent.esM = update_parameter(
                            component=demand_sink,
                            esM=self.parent.esM,
                            updated_parameter_dict={
                                "capacityMin": None,
                                "capacityMax": None,
                                "capacityFix": temp_dem})

                # buildings
                else:
                    if demand_sink.name in self.parent.demand.index:
                        temp_dem = self.parent.demand.loc[
                            demand_sink.name, self.parent.currentyear]
                        self.parent.esM = update_parameter(
                            component=demand_sink,
                            esM=self.parent.esM,
                            updated_parameter_dict={
                                "capacityMin": None,
                                "capacityMax": None,
                                "capacityFix": temp_dem})

    def initBackcasting(self):
        """Calculate sCurve for backcasting transformation path analysis."""
        if not self.parent.targetyearoptimization:
            raise ValueError("Wrong year for initBackcasting")
        years = range(self.parent.refyear, self.parent.targetyear + 1)
        sCurve_lb = pd.DataFrame(index=years)
        sCurve_ub = pd.DataFrame(index=years)

        # node list
        listNodes = [x for x in self.parent.esM.componentNames]

        # define sCurves for all components
        for component_name in self.parent.esM.componentNames:
            component = self.parent.esM.getComponent(component_name)
            # for unrestricted components -> just ub and lb
            if component.name in self.unrestricted_components:
                # Special case for unrestricted components with historical data
                if "_stock" in component.name:
                    pass
                elif component.name+"_stock" in listNodes:
                    stock_component = self.parent.esM.getComponent(
                        component.name+"_stock")
                    sCurve_lb[component.name] = component.capacityMin[self.parent.location] + \
                        stock_component.capacityFix[self.parent.location]
                    sCurve_ub[component.name] = component.capacityMax[self.parent.location] + \
                        stock_component.capacityFix[self.parent.location]

                # normal case
                else:
                    if component.capacityFix is not None and component.capacityMax is None:
                        sCurve_lb[component.name] = component.capacityFix[self.parent.location]
                        sCurve_ub[component.name] = component.capacityFix[self.parent.location]
                    else:
                        sCurve_lb[component.name] = component.capacityMin[self.parent.location]
                        sCurve_ub[component.name] = component.capacityMax[self.parent.location]

            # for forced decomissioning components
            elif component.name in self.parent.forceddecommissioning.columns:
                sCurve_ub[component.name] = self.parent.forceddecommissioning[
                    component.name]
                sCurve_lb[component.name] = 0

            # restricted componentscomponent
            else:
                # pass for stock
                if "_stock" in component.name or "CO2Environment" in component.name:
                    continue
                # not stock components
                ###############################################################
                # 1. get entire installed capacity in target year: stock + new
                # 1a) result of stock component in target year (if existing)
                if component.name + '_stock' in listNodes:
                    stock_capacity = self.parent.esM.getComponent(
                        component.name + '_stock').capacityFix[self.parent.location]
                    if stock_capacity < 0.01:
                        stock_capacity = 0
                else:
                    stock_capacity = 0

                # 1b) result of new installed component in target year
                # Problem if capacityFix==0?
                result_df = self.parent.esM.componentModelingDict[component.modelingClass.__name__].getOptimalValues(
                    name='capacityVariablesOptimum')['values']
                if component.name in result_df.index:
                    result = result_df.loc[component.name,
                                           self.parent.location]
                else:
                    result = component.capacityFix[self.parent.location]
                new_component_capacity = result

                # 1c) technology capacity in tartget year
                component_capacity_targetyear = \
                    stock_capacity + new_component_capacity

                ##############################################################
                # 2) Definition of sCurve Lower Bound
                # 2.1) not installed restricted components
                if component_capacity_targetyear < 0.01:
                    sCurve_lb[component.name] = 0

                # 2.2) installed restricted components
                else:
                    # 2a) Get starting end and end capacity
                    # Start capacity
                    if component.name in self.parent.historicalcapacity:
                        start_capacity = self.parent.historicalcapacity.loc[
                            self.parent.refyear, component.name]
                    else:
                        start_capacity = 0
                    # End capacity
                    end_capacity = component_capacity_targetyear

                    # Set sCurve Start and end capacity
                    sCurve_lb.loc[self.parent.refyear,
                                  component.name] = start_capacity
                    sCurve_lb.loc[self.parent.targetyear,
                                  component.name] = end_capacity

                    if start_capacity >= end_capacity:
                        start_capacity = 0

                    # sCurve parameters
                    # param_A logistic function target: +-x%
                    param_A = self.parent.sCurveParam[0]
                    param_B = self.parent.sCurveParam[1]
                    param_C = self.parent.sCurveParam[2] / \
                        (end_capacity - start_capacity)
                    shift = 0

                    for year in range(self.parent.refyear + 1, self.parent.targetyear):
                        # calculation of lb scurve function
                        sCurve_lb.loc[year, component.name] = ((param_A * (end_capacity - start_capacity)) /
                                                               (1 + param_A * param_C * (end_capacity - start_capacity) * np.exp(-param_B * (year - self.parent.refyear)))) + start_capacity

                        # special treatment for buildings
                        # TODO Masterthesis later
                        if any(k in component.name for k in self.parent.string_identifiers["building_categories"]):
                            if not self.parent.string_identifiers["building_filter_for_helper_components"] in component.name:
                                # linear
                                # sCurve_lb.loc[year, component.name] = (end_capacity-start_capacity)*(year-self.parent.refyear)/(self.parent.targetyear-self.parent.refyear)+start_capacity
                                # min. lb
                                sCurve_lb.loc[year, component.name] = self.parent.buildingdata.loc[component.name, 'Netto-Sanierungsquote'] * \
                                    (year - self.parent.refyear)
                                if (sCurve_lb.loc[year, component.name] > end_capacity) and (self.parent.string_identifiers["Sanierungspaket1"] in component.name):
                                    sCurve_lb[component.name][
                                        self.parent.targetyear] = 0
                                    sCurve_lb.loc[year, component.name] = 0

                            # if the yearly value exceeds the end capacity,
                            # shift the scurve by one year
                            if sCurve_lb.loc[year, component.name] > end_capacity:
                                shift += 1

                    # if lower bounds of s-curve exceed end capacity,
                    # shift the lower scurve towards the end
                    if shift > 0:
                        sCurve_lb.loc[self.parent.refyear + shift +
                                      1:, component.name] = sCurve_lb.loc[self.parent.refyear + 1:-shift, component.name]
                        sCurve_lb.loc[:shift, component.name] = start_capacity

                ##############################################################
                # 3. Definition of sCurve Upper bound
                # 3a) for components with more historical capaicty than ub
                if component.name in self.parent.historicalcapacity.columns:
                    # historical capacity higher than ub -> correct years
                    if self.parent.historicalcapacity.loc[self.parent.refyear, component.name] > self.raw_ub[component.name]:
                        for year in sCurve_ub.index:
                            if self.parent.historicalcapacity.loc[year, component.name] > self.raw_ub[component.name]:
                                sCurve_ub.loc[year, component.name] = self.parent.historicalcapacity.loc[
                                    year, component.name]
                            else:
                                sCurve_ub.loc[year, component.name] = self.raw_ub[
                                    component.name]
                    else:
                        sCurve_ub[component.name] = self.raw_ub[
                            component.name]
                # 3b) rest
                elif "Virt" in component.name:
                    pass
                else:
                    sCurve_ub[component.name] = self.raw_ub.loc[component.name]

        ######################################################################
        # no forced expansion of "inefficient" reference buildings
        for nn in sCurve_lb.columns:
            if any(h in nn for h in self.parent.string_identifiers["building_categories"]) and not any(hh in nn for hh in self.parent.string_identifiers["building_standards"]):
                sCurve_lb[nn] = 0

        ######################################################################
        # Restrict renewables if applied
        if self.parent.considerMaxYearlyExpansionLimit:
            # get result of renewable tech groups in targetyear
            self.ee_result_targetyear = pd.Series()
            for tech in ["onshore", "offshore", "rooftop_pv", "openfield_pv"]:
                self.ee_result_targetyear[tech] = 0
                tech_items = [
                    x for x in self.parent.esM.componentNames if self.parent.string_identifiers[tech] in x]
                for tech_item in tech_items:
                    component = self.parent.esM.getComponent(tech_item)
                    result_df = \
                        self.parent.esM.componentModelingDict[
                            component.modelingClass.__name__].getOptimalValues(
                            name='capacityVariablesOptimum')['values']
                    if component.name in result_df.index:
                        result = result_df.loc[component.name,
                                               self.parent.location]
                    else:
                        result = component.capacityFix[self.parent.location]

                    self.ee_result_targetyear[tech] = \
                        self.ee_result_targetyear[tech]+result
            print("\n\n ")
            print(self.ee_result_targetyear)
            self.getSharedExpansionCapacityMinRenewables()
        ######################################################################
        # Round the scurves
        sCurve_lb = _round(sCurve_lb)
        sCurve_ub = _round(sCurve_ub)

        ######################################################################
        # Raising Errors, checks
        if sCurve_lb.isnull().values.any():
            print("\n\nsCurve_lb contains None-values. Columns: " +
                  "{}".format(sCurve_lb.columns[sCurve_lb.isna().any()].tolist()))
            print("CHANGE ABOVE  TO RAISEERROR")
            # raise ValueError("sCurve_lb contains None-values. Columns: " +
            #                  "{}".format(sCurve_lb.columns[sCurve_lb.isna().any()].tolist()))
        if sCurve_ub.isnull().values.any():
            print("\n\nsCurve_ub contains None-values. Columns: " +
                  "{}".format(sCurve_ub.columns[sCurve_ub.isna().any()].tolist()))
            print("CHANGE ABOVE  TO RAISEERROR")
            # raise ValueError("sCurve_ub contains None-values. Columns: " +
            #                  "{}".format(sCurve_ub.columns[sCurve_ub.isna().any()].tolist()))

        for component_name in self.parent.esM.componentNames:
            component = self.parent.esM.getComponent(component_name)
            if "_stock" in component.name or "Virt" in component.name or "CO2Environment" in component.name:
                continue

            # check minimum value in sCurve_lb
            if sCurve_lb[component.name].min() < 0:
                raise Warning("Calculated capacity for component " +
                              "{} is lower than 0".format(component.name) +
                              "Revise s-curve formulation or params.")

            # raise error if sCurve exceeds raw ub in target year
            if component.name in self.raw_ub:
                if _round(sCurve_ub.loc[self.parent.targetyear, component.name]) > _round(self.raw_ub.loc[component.name])+0.0001:
                    raise ValueError(
                        "Component '{}': sCurve_ub ({}) exceeds ub ({}) in target year".format(
                            component.name,
                            sCurve_ub.loc[self.parent.targetyear,
                                          component.name],
                            self.raw_ub.loc[component.name]))
                if sCurve_lb.loc[self.parent.targetyear, component.name] > self.raw_ub.loc[component.name]:
                    raise ValueError(
                        "Component '{}': sCurve_lb ({}) exceeds ub ({}) in target year".format(
                            component.name,
                            sCurve_lb.loc[self.parent.targetyear,
                                          component.name],
                            self.raw_ub.loc[component.name]))

            else:
                if component.capacityMax is None and component.capacityFix is not None:
                    _capacity_value = component.capacityFix[self.parent.location]
                elif component.capacityFix is None and component.capacityMax is not None:
                    _capacity_value = component.capacityMax[self.parent.location]
                else:
                    raise ValueError(
                        "capacityMax and capacityFix defined for "+component.name)
                if sCurve_ub.loc[self.parent.targetyear, component.name] > _capacity_value:
                    raise ValueError("Component '{}': sCurve_ub ({}) exceeds ub ({}) in target year".format(
                        component.name,
                        sCurve_ub.loc[self.parent.targetyear, component.name],
                        self.raw_ub.loc[component.name]))
                if sCurve_lb.loc[self.parent.targetyear, component.name] > _capacity_value:
                    raise ValueError("Component '{}': sCurve_lb ({}) exceeds ub ({}) in target year".format(
                        component.name,
                        sCurve_lb.loc[self.parent.targetyear, component.name],
                        self.raw_ub.loc[component.name]))

            # raise warning if scurve lb exceeds raw_ub -> for forced
            # components this happens in first model years and is ok
            if component.name in self.raw_ub:
                error_raw_ub = False
                for year in sCurve_ub.index:
                    if sCurve_lb.loc[year, component.name] > self.raw_ub.loc[component.name]:
                        error_raw_ub = True
                if error_raw_ub == True:
                    print(sCurve_lb[component.name])
                    raise ValueError("Component '{}': sCurve_lb exceeds ub ({}) in year {}".format(
                        component.name, self.raw_ub.loc[component.name], year))

            # check that ub is higher than lb
            if component.name in sCurve_ub.columns and component.name in sCurve_lb.columns:
                error = False
                for year in sCurve_ub.index:
                    if sCurve_ub.loc[year, component.name] < sCurve_lb.loc[year, component.name]:
                        error = True
                        print(
                            "\nProblem with setting up sCurve of " +
                            "'{}' in year '{}'".format(component.name, year))
                        print("sCurve ub: {}".format(
                            sCurve_ub.loc[year, component.name]))
                        print("sCurve lb: {}".format(
                            sCurve_lb.loc[year, component.name]))
            if error is True:
                raise ValueError(
                    "sCurve_lb exceeds sCurve_ub for component '{}'".format(component.name))

        # export temp files
        sCurve_lb.to_excel(os.path.join(
            self.parent.temppath, 'temp_scurve_lb.xlsx'))
        sCurve_ub.to_excel(os.path.join(
            self.parent.temppath, 'temp_scurve_ub.xlsx'))
        return

    def fixNumericalInstabilities(self):
        for component_name in self.parent.esM.componentNames:
            component = self.parent.esM.getComponent(component_name)
            if component.capacityMax is not None:
                # round to four decimal places
                component.capacityMax[self.parent.location] = _round(
                    component.capacityMax[self.parent.location])
                component.capacityMin[self.parent.location] = _round(
                    component.capacityMin[self.parent.location])

            # 1. check negativitiy
            if component.capacityMax is not None:
                if component.capacityMax[self.parent.location] < 0:
                    raise ValueError("Component '{}'. ".format(component.name) +
                                     "with negative upper bound: {}".format(component.capacityMax[self.parent.location]))
                if component.capacityMin[self.parent.location] < 0:
                    raise ValueError("Component '{}'. ".format(component.name) +
                                     "with negative lower bound: {}".format(component.capacityMin[self.parent.location]))
            if component.capacityFix is not None:
                if component.capacityFix[self.parent.location] < 0:
                    raise ValueError("Component '{}'. ".format(component.name) +
                                     "with negative fix capacity: {}".format(component.capacityFix[self.parent.location]))

    def getSharedExpansionCapacityMinRenewables(self):
        temp_stock_decommission = pd.read_excel(
            os.path.join(self.parent.temppath, 'temp_stock_decommission.xlsx'),
            index_col=0)
        temp_stock_capacity = pd.read_excel(
            os.path.join(self.parent.temppath, 'temp_stock_capacity.xlsx'),
            index_col=0)

        self.yearlyRenewableExpansionMin = pd.DataFrame(
            columns=["onshore", "offshore", "openfield_pv", "rooftop_pv"],
            index=range(self.parent.refyear+1, self.parent.targetyear+1, 1))
        min_installed_capacity = pd.DataFrame(
            columns=["onshore", "offshore", "openfield_pv", "rooftop_pv"])

        for tech in ["onshore", "offshore", "openfield_pv", "rooftop_pv"]:
            tech_items = [
                x for x in self.parent.esM.componentNames if self.parent.string_identifiers[tech] in x]
            tech_items_not_stock = [
                x for x in tech_items if "_stock" not in x]
            tech_stock = temp_stock_capacity[tech_items_not_stock].sum(
                axis=1)
            switch_year = None  # year from which on renewables needs to be installed with highest speed to ensure to keep expansion limitation and achieve GW in target yera
            for year in reversed(range(self.parent.refyear+1, self.parent.targetyear+1, 1)):
                # get parameters for renewable techs

                if tech == "onshore":
                    yearlyRestriction = self.parent.maxYearlyExpansionGW_Onshore
                elif tech == "offshore":
                    yearlyRestriction = self.parent.maxYearlyExpansionGW_Offshore
                elif tech == "openfield_pv":
                    yearlyRestriction = self.parent.maxYearlyExpansionGW_OFPV
                elif tech == "rooftop_pv":
                    yearlyRestriction = self.parent.maxYearlyExpansionGW_RTPV
                else:
                    raise ValueError(
                        f"What is the expansion rate for '{tech}?'")

                # estimate the minimal installed over transformation pathway
                # required capacity by
                # considering the goal (target year)
                # 1) for the target year -> result
                if year == self.parent.targetyear:
                    min_installed_capacity.loc[year, tech] =\
                        self.ee_result_targetyear[tech]\
                        - yearlyRestriction \
                        + temp_stock_decommission.loc[year, tech_items_not_stock].sum()
                    if min_installed_capacity.loc[year, tech] < 0:
                        min_installed_capacity.loc[year, tech] = 0
                # 2) for all other years
                else:
                    min_installed_capacity.loc[year, tech] =\
                        min_installed_capacity.loc[year+1, tech]\
                        - yearlyRestriction \
                        + temp_stock_decommission.loc[year, tech_items_not_stock].sum()
                    if min_installed_capacity.loc[year, tech] < 0:
                        print("\ntech")
                        print(tech)
                        print("min_installed_capacity.loc[:, tech]")
                        print(min_installed_capacity.loc[:, tech])
                        print("yearlyRestriction")
                        print(yearlyRestriction)
                        print(
                            "temp_stock_decommission.loc[year, tech_items_not_stock].sum()")
                        print(
                            temp_stock_decommission.loc[year, tech_items_not_stock].sum())
                        print("year")
                        print(year)
                        raise ValueError("Min installed capacity is negative.")
                    if min_installed_capacity.loc[year, tech] <= tech_stock[year]:
                        min_installed_capacity.loc[year, tech] = 0
                        switch_year = year
                        break
            if switch_year == None:
                switch_year = self.parent.refyear
                if min_installed_capacity.loc[self.parent.refyear+1, tech] - yearlyRestriction <= tech_stock[self.parent.refyear]:
                    raise ValueError("Calculation is wrong")

            # write it to df
            self.yearlyRenewableExpansionMin.loc[
                self.parent.refyear:switch_year, tech] = 0
            self.yearlyRenewableExpansionMin.loc[
                switch_year:self.parent.targetyear, tech] = \
                yearlyRestriction

    def check_if_lb_greater_ub(self):
        for component_name in self.parent.esM.componentNames:
            component = self.parent.esM.getComponent(component_name)
            if component.capacityMax is not None:
                _check_if_lb_greater_ub(
                    component.capacityMin[self.parent.location],
                    component.capacityMax[self.parent.location],
                    component.name)


def _check_if_lb_greater_ub(lb, ub, name):
    if lb > ub:
        raise ValueError(
            "Lower bound '{}' greater than ".format(lb) +
            "upper bound '{}' ".format(ub) +
            "for component '{}'".format(name))


def _round(data):
    if data is not None:
        data = round(data, 4)
    return data


def check_existance_of_renewable_techs(tech, naminglist):
    if len(naminglist) == 0:
        raise ValueError(
            "Cannot find components for {}".format(tech) +
            "Check the string identifier in the json file!")


def check_for_infeasibility(terminination_string, year):
    if terminination_string in ['infeasible', "infeasibleOrUnbounded"]:
        raise ValueError(
            f"Optimization of year  {year} is infeasible.")
