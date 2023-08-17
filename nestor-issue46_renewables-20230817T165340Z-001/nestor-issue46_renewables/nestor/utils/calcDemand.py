import pandas as pd
import os


def calculate_demand(year, resfolderpath, FINEResults, esM):
    flow_folder_name = str(year)
    resfolderpath_flow = os.path.join(resfolderpath, flow_folder_name)
    if not os.path.exists(resfolderpath_flow):
        os.makedirs(resfolderpath_flow)
    get_demand_data_CH4(FINEResults, year, resfolderpath_flow, esM)
    get_demand_data_H2(FINEResults, year, resfolderpath_flow, esM)
    get_demand_data_power(FINEResults, year, resfolderpath_flow, esM)
    return FINEResults


def get_demand_data_CH4(FINEResults, year, resfolderpath_flow, esM):
    '''
    Get demand timeseries for CH4 Components

    The function distinguishes between the start year (2020)
    and all the other comming free opimization years.
    it checks all components of the energy system if these are related
    to the CH4 commodity.
    It checks if the analysed componet is connected to the output of
    the CH4 Grid, by looking for the conversion factor (ConvFac) CH4Hub2.
    If this data is negativ, then it means the component consumes CH4
    and thus it need to be considered for the demands.
    In 2020 these is just a operation optimization of the system thus only
    already in 2020 installed components can be checked.

    '''
    df_CH4 = pd.DataFrame()
    for component in esM.componentNames:

        if any(x in component for x in ["-Src-", "TSource", "TSink"]):
            continue

        if esM.componentNames[component] == 'ConversionModel':

            General_operation = FINEResults[year][esM.componentNames[component]
                                                  ].loc[component].loc['operation'].values[0][0]
            if str(General_operation) == 'nan':
                pass

            else:
                for ConvFac_name in esM.getComponentAttribute(component, 'commodityConversionFactors'):

                    if ConvFac_name == 'P-Hub-CH4Hub-CH4':
                        ConvFac_value = esM.getComponentAttribute(
                            component, 'commodityConversionFactors')[ConvFac_name]
                        data = esM.componentModelingDict['ConversionModel'].getOptimalValues(
                            name='operationVariablesOptimum')['values'].loc[component, 'GermanyRegion']
                        data_conv = ConvFac_value * data

                        if data_conv[0] < 0:

                            data_conv.name = ConvFac_name+' to ' + component
                            data_conv = data_conv.abs()
                            df_CH4 = pd.concat([df_CH4, data_conv], axis=1)

        elif esM.componentNames[component] == 'StorageModel':

            General_operation = FINEResults[year][esM.componentNames[component]
                                                  ].loc[component].loc['operationCharge'].values[0][0]
            if str(General_operation) == 'nan':
                pass

            else:
                if esM.getComponentAttribute(component, 'commodity') == 'P-Hub-CH4Hub-CH4':
                    data = esM.componentModelingDict['StorageModel'].getOptimalValues(
                        name='chargeOperationVariablesOptimum')['values'].loc[component, 'GermanyRegion']
                    data.name = esM.getComponentAttribute(
                        component, 'commodity')+' to ' + component
                    data = data.abs()
                    df_CH4 = pd.concat([df_CH4, data], axis=1)

    # Sum up and drop Stocks
    for flows in df_CH4.keys():

        if flows[-6:] == '_stock':

            if flows[0:-6] in df_CH4.keys():

                temp = df_CH4[flows]+df_CH4[flows[0:-6]]
                df_CH4 = df_CH4.drop([flows], axis=1)
                df_CH4 = df_CH4.drop(flows[0:-6], axis=1)
                df_CH4[flows[0:-6]] = temp

            else:
                temp = df_CH4[flows]
                df_CH4 = df_CH4.drop([flows], axis=1)
                df_CH4[flows[0:-6]] = temp

    # Save Timeseries
    df_CH4.to_excel(resfolderpath_flow+"/CH4-Flows.xlsx",
                    sheet_name='CH4Hub2OUT')

    return


def get_demand_data_H2(FINEResults, year, resfolderpath_flow, esM):
    '''
    Get demand timeseries for H2 Components

    The function distinguishes between the start year (2020)
    and all the other comming free opimization years. It also
    distinguishes between hydrogen components which are conneted
    to the transfer grid and components connected to the distribution grid.
    It checks all components of the energy system if these are related
    to the H2 commodity.
    It checks if the analysed componet is connected to the output of
    the H2 transgrid or H2 distributiongrid , by looking for the conversion
    factor (ConvFac) H2GridHub and HH-H2Hub.
    If this data is negativ, then it means the component consumes H2
    and thus it need to be considered for the demands.
    In 2020 these is just a operation optimization of the system thus only
    already in 2020 installed components can be checked.

    '''
    df_H2_GridHub = pd.DataFrame()
    df_H2Hub = pd.DataFrame()
    for component in esM.componentNames:

        if any(x in component for x in ["-Src-", "TSource", "TSink"]):
            continue

        if esM.componentNames[component] == 'ConversionModel':

            General_operation = FINEResults[year][esM.componentNames[component]
                                                  ].loc[component].loc['operation'].values[0][0]

            if str(General_operation) == 'nan':
                pass

            else:
                for ConvFac_name in esM.getComponentAttribute(component, 'commodityConversionFactors'):

                    if (ConvFac_name == 'P-Hub-H2GridHub-H2') or ('B-Hub-HouseHoldH2Hub-H2' in ConvFac_name):

                        ConvFac_value = esM.getComponentAttribute(
                            component, 'commodityConversionFactors')[ConvFac_name]
                        data = esM.componentModelingDict['ConversionModel'].getOptimalValues(
                            name='operationVariablesOptimum')['values'].loc[component, 'GermanyRegion']
                        data_conv = ConvFac_value * data

                        if data_conv[0] < 0:

                            data_conv.name = ConvFac_name+' to '+component
                            data_conv = data_conv.abs()

                            if ConvFac_name == 'P-Hub-H2GridHub-H2':
                                df_H2_GridHub = pd.concat(
                                    [df_H2_GridHub, data_conv], axis=1)
                            elif 'B-Hub-HouseHoldH2Hub-H2' in ConvFac_name:
                                df_H2Hub = pd.concat(
                                    [df_H2Hub, data_conv], axis=1)

        elif esM.componentNames[component] == 'StorageModel':

            General_operation = FINEResults[year][esM.componentNames[component]
                                                  ].loc[component].loc['operationCharge'].values[0][0]

            if str(General_operation) == 'nan':
                pass

            else:
                if (esM.getComponentAttribute(component, 'commodity') == 'P-Hub-H2GridHub-H2') or ('B-Hub-HouseHoldH2Hub-H2' in esM.getComponentAttribute(component, 'commodity')):

                    data = esM.componentModelingDict['StorageModel'].getOptimalValues(
                        name='chargeOperationVariablesOptimum')['values'].loc[component, 'GermanyRegion']
                    data.name = esM.getComponentAttribute(
                        component, 'commodity')+' to ' + component
                    data = data.abs()

                    if esM.getComponentAttribute(component, 'commodity') == 'P-Hub-H2GridHub-H2':
                        df_H2_GridHub = pd.concat(
                            [df_H2_GridHub, data], axis=1)
                    elif 'B-Hub-HouseHoldH2Hub-H2' in esM.getComponentAttribute(component, 'commodity'):
                        df_H2Hub = pd.concat([df_H2Hub, data], axis=1)

        elif esM.componentNames[component] == 'SourceSinkModel':

            General_operation = FINEResults[year][esM.componentNames[component]
                                                  ].loc[component].loc['operation'].values[0][0]

            if str(General_operation) == 'nan':
                pass

            else:
                if (esM.getComponentAttribute(component, 'commodity') == 'P-Hub-H2GridHub-H2') or ('B-Hub-HouseHoldH2Hub-H2' in esM.getComponentAttribute(component, 'commodity')):
                    data = esM.componentModelingDict['SourceSinkModel'].getOptimalValues(
                        name='operationVariablesOptimum')['values'].loc[component, 'GermanyRegion']
                    data.name = esM.getComponentAttribute(
                        component, 'commodity')+' to '+component
                    data = data.abs()

                    if esM.getComponentAttribute(component, 'commodity') == 'P-Hub-H2GridHub-H2':
                        df_H2_GridHub = pd.concat(
                            [df_H2_GridHub, data], axis=1)
                    elif 'B-Hub-HouseHoldH2Hub-H2' in esM.getComponentAttribute(component, 'commodity'):
                        df_H2Hub = pd.concat([df_H2Hub, data], axis=1)

    # Sum up and drop Stocks
    for flows in df_H2_GridHub.keys():

        if flows[-6:] == '_stock':

            if flows[0:-6] in df_H2_GridHub.keys():

                temp = df_H2_GridHub[flows]+df_H2_GridHub[flows[0:-6]]
                df_H2_GridHub = df_H2_GridHub.drop([flows], axis=1)
                df_H2_GridHub = df_H2_GridHub.drop(flows[0:-6], axis=1)
                df_H2_GridHub[flows[0:-6]] = temp

            else:
                temp = df_H2_GridHub[flows]
                df_H2_GridHub = df_H2_GridHub.drop([flows], axis=1)
                df_H2_GridHub[flows[0:-6]] = temp

    for flows in df_H2Hub.keys():

        if flows[-6:] == '_stock':

            if flows[0:-6] in df_H2Hub.keys():

                temp = df_H2Hub[flows]+df_H2Hub[flows[0:-6]]
                df_H2Hub = df_H2Hub.drop([flows], axis=1)
                df_H2Hub = df_H2Hub.drop(flows[0:-6], axis=1)
                df_H2Hub[flows[0:-6]] = temp

            else:
                temp = df_H2Hub[flows]
                df_H2Hub = df_H2Hub.drop([flows], axis=1)
                df_H2Hub[flows[0:-6]] = temp

    # Save Timeseries
    with pd.ExcelWriter(resfolderpath_flow+"/H2-Flows.xlsx") as writer:
        df_H2_GridHub.to_excel(writer, sheet_name='H2GridHubOUT')
        df_H2Hub.to_excel(writer, sheet_name='H2HubOUT')

    return


def get_demand_data_power(FINEResults, year, resfolderpath_flow, esM):
    '''
    Get demand timeseries for electricity Components

    The function distinguishes between the start year (2020)
    and all the other comming free opimization years. It also
    distinguishes between electicity components which are conneted
    to the distribution grid and components connected to the houshold grid.
    It checks all components of the energy system if these are related
    to the electricity commodity.
    It checks if the analysed componet is connected to the output of
    the electricity distribution or houehold grid , by looking for the conversion
    factor (ConvFac) Demand-EHub and HH-EHub.
    If this data is negativ, then it means the component consumes electricity
    and thus it need to be considered for the demands.
    In 2020 these is just a operation optimization of the system thus only
    already in 2020 installed components can be checked.

    '''
    df_Demand_EHub = pd.DataFrame()
    df_HH_EHub = pd.DataFrame()

    for component in esM.componentNames:

        if any(x in component for x in ["-Src-", "TSource", "TSink"]):
            continue

        if esM.componentNames[component] == 'ConversionModel':

            General_operation = FINEResults[year][esM.componentNames[component]
                                                  ].loc[component].loc['operation'].values[0][0]

            if str(General_operation) == 'nan':
                pass

            else:
                for ConvFac_name in esM.getComponentAttribute(component, 'commodityConversionFactors'):

                    if (ConvFac_name == 'P-Hub-DemandEHub-el') or (ConvFac_name == 'B-Hub-HouseHoldEHub-el'):
                        if isinstance(esM.getComponentAttribute(component, 'commodityConversionFactors')[ConvFac_name], pd.Series):
                            ConvFac_value = esM.getComponentAttribute(
                                component, 'commodityConversionFactors')[ConvFac_name].mean()
                            data = esM.componentModelingDict['ConversionModel'].getOptimalValues(
                                name='operationVariablesOptimum')['values'].loc[component, 'GermanyRegion']
                            data_conv = ConvFac_value * data

                        else:
                            ConvFac_value = esM.getComponentAttribute(
                                component, 'commodityConversionFactors')[ConvFac_name]
                            data = esM.componentModelingDict['ConversionModel'].getOptimalValues(
                                name='operationVariablesOptimum')['values'].loc[component, 'GermanyRegion']
                            data_conv = ConvFac_value * data

                        if data_conv[0] < 0:
                            data_conv.name = ConvFac_name+' to '+component
                            data_conv = data_conv.abs()

                            if ConvFac_name == 'P-Hub-DemandEHub-el':
                                df_Demand_EHub = pd.concat(
                                    [df_Demand_EHub, data_conv], axis=1)
                            elif ConvFac_name == 'B-Hub-HouseHoldEHub-el':
                                df_HH_EHub = pd.concat(
                                    [df_HH_EHub, data_conv], axis=1)

        elif esM.componentNames[component] == 'SourceSinkModel':

            General_operation = FINEResults[year][esM.componentNames[component]
                                                  ].loc[component].loc['operation'].values[0][0]

            if str(General_operation) == 'nan':
                pass

            else:
                if (esM.getComponentAttribute(component, 'commodity') == 'Demand-EHub') or (esM.getComponentAttribute(component, 'commodity') == 'HH-EHub'):
                    data = esM.componentModelingDict['SourceSinkModel'].getOptimalValues(
                        name='operationVariablesOptimum')['values'].loc[component, 'GermanyRegion']
                    data.name = esM.getComponentAttribute(
                        component, 'commodity')+' to '+component
                    data = data.abs()

                    if esM.getComponentAttribute(component, 'commodity') == 'P-Hub-DemandEHub-el':
                        df_Demand_EHub = pd.concat(
                            [df_Demand_EHub, data], axis=1)
                    elif esM.getComponentAttribute(component, 'commodity') == 'B-Hub-HouseHoldEHub-el':
                        df_HH_EHub = pd.concat([df_HH_EHub, data], axis=1)

    # Sum up and drop Stocks
    for flows in df_Demand_EHub.keys():

        if flows[-6:] == '_stock':

            if flows[0:-6] in df_Demand_EHub.keys():

                temp = df_Demand_EHub[flows]+df_Demand_EHub[flows[0:-6]]
                df_Demand_EHub = df_Demand_EHub.drop([flows], axis=1)
                df_Demand_EHub = df_Demand_EHub.drop(flows[0:-6], axis=1)
                df_Demand_EHub[flows[0:-6]] = temp

            else:
                temp = df_Demand_EHub[flows]
                df_Demand_EHub = df_Demand_EHub.drop([flows], axis=1)
                df_Demand_EHub[flows[0:-6]] = temp

    for flows in df_HH_EHub.keys():

        if flows[-6:] == '_stock':

            if flows[0:-6] in df_HH_EHub.keys():

                temp = df_HH_EHub[flows]+df_HH_EHub[flows[0:-6]]
                df_HH_EHub = df_HH_EHub.drop([flows], axis=1)
                df_HH_EHub = df_HH_EHub.drop(flows[0:-6], axis=1)
                df_HH_EHub[flows[0:-6]] = temp

            else:
                temp = df_HH_EHub[flows]
                df_HH_EHub = df_HH_EHub.drop([flows], axis=1)
                df_HH_EHub[flows[0:-6]] = temp

    # Save Timeseries
    with pd.ExcelWriter(resfolderpath_flow+"/E-Flows.xlsx") as writer:
        df_Demand_EHub.to_excel(writer, sheet_name='DemGridOUT')
        df_HH_EHub.to_excel(writer, sheet_name='HHGridOUT')

    return
