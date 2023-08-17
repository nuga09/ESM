import pandas as pd


def get_yearly_FINE_results(FINEResults: dict, esM, year):
    FINEResults[year] = {}
    FINEResults[year]['ConversionModel'] = \
        esM.getOptimizationSummary('ConversionModel')
    FINEResults[year]['SourceSinkModel'] = \
        esM.getOptimizationSummary('SourceSinkModel')
    FINEResults[year]['StorageModel'] = \
        esM.getOptimizationSummary('StorageModel')
    return FINEResults


def export_FINE_results(anyears: list, FINEResults, esM, resultfilepath: str):
    """Export the results of FINE.

    Parameters
    ----------
    anyears : list
        [description]
    FINEResults : [type]
        [description]
    esM : [type]
        [description]
    resultfilepath : str
        path to the results folder
    """
    # Export transformation path analysis results
    modelyears = ["target year"]
    if len(anyears) > 2:
        modelyears.extend(anyears)
    capacities = pd.DataFrame()
    operations = pd.DataFrame()
    totalcosts = pd.DataFrame()
    emissions = pd.DataFrame()
    for year in modelyears:
        # TODO loop over optimization summary?
        for comp_name in esM.componentNames:
            if comp_name in FINEResults[year][esM.componentNames[comp_name]].loc[(slice(None), 'capacity'), 'GermanyRegion'].index:
                # extract component infos for model year
                # TODO why does comp_info contain all components?
                comp_infos = FINEResults[year][esM.componentNames[comp_name]]

                # write capacity data
                capacities.loc[comp_name, year] = comp_infos.loc[
                    comp_name, 'capacity'].iloc[0, 0]

                # write operation data
                if esM.componentNames[comp_name] == 'StorageModel':
                    Hub = esM.getComponentAttribute(comp_name, 'commodity')
                    operations.loc['Charge+' + Hub + '+' + comp_name, year] = \
                        comp_infos.loc[comp_name, 'operationCharge'].iloc[0, 0]
                    operations.loc['DisCharge+' + Hub + '+' + comp_name, year] = \
                        comp_infos.loc[comp_name,
                                       'operationDischarge'].iloc[0, 0]
                elif esM.componentNames[comp_name] == 'ConversionModel':
                    for ConvFac in esM.getComponentAttribute(comp_name, 'commodityConversionFactors'):
                        if ConvFac == 'CO2Out':
                            pass
                        elif isinstance(esM.getComponentAttribute(comp_name, 'commodityConversionFactors')[ConvFac], pd.Series):
                            ConvFac_Num = esM.getComponentAttribute(
                                comp_name, 'commodityConversionFactors')[ConvFac].mean()
                            operations.loc[ConvFac + '+' + comp_name, year] = \
                                ConvFac_Num * \
                                comp_infos.loc[comp_name,
                                               'operation'].iloc[0, 0]
                        else:
                            ConvFac_Num = esM.getComponentAttribute(
                                comp_name, 'commodityConversionFactors')[ConvFac]
                            operations.loc[ConvFac + '+' + comp_name, year] = \
                                ConvFac_Num * \
                                comp_infos.loc[comp_name,
                                               'operation'].iloc[0, 0]
                else:
                    operations.loc[comp_name, year] = comp_infos.loc[
                        comp_name, 'operation'].iloc[0, 0]

                # write costs data
                totalcosts.loc[comp_name, year] = comp_infos.loc[
                    comp_name, 'TAC'].iloc[0, 0]

                # write emission data
                if esM.componentNames[comp_name] == 'ConversionModel':
                    if 'CO2Out' in esM.getComponentAttribute(comp_name, 'commodityConversionFactors'):
                        CO2_ConFac = esM.getComponentAttribute(
                            comp_name, 'commodityConversionFactors')['CO2Out']
                    else:
                        CO2_ConFac = 0
                else:
                    CO2_ConFac = 0
                if esM.componentNames[comp_name] == 'StorageModel':
                    emissions.loc['Charge_' + comp_name, year] = CO2_ConFac * \
                        comp_infos.loc[comp_name, 'operationCharge'].iloc[0, 0]
                    emissions.loc['DisCharge_' + comp_name, year] = CO2_ConFac * \
                        comp_infos.loc[comp_name,
                                       'operationDischarge'].iloc[0, 0]
                else:
                    emissions.loc[comp_name, year] = CO2_ConFac * \
                        comp_infos.loc[comp_name, 'operation'].iloc[0, 0]
            else:
                pass

    # iterate over dataframes: add 0, add stock to component, then drop stock
    def clean_df(df):
        df = df.fillna(0)
        for r in df.index:
            if '_stock' in r:
                df.loc[r.replace("_stock", "")] += df.loc[r]
                df = df.drop([r])
        return df
    capacities = clean_df(capacities)
    operations = clean_df(operations)
    totalcosts = clean_df(totalcosts)
    emissions = clean_df(emissions)

    # rename flow to Auswertefile-Layout
    for flow_name_FINE in operations.index:
        if '+' in flow_name_FINE:
            wordparts = flow_name_FINE.split('+')
            if wordparts[0] == 'DisCharge' or wordparts[0] == 'Charge':
                if 'DisCharge' == wordparts[0]:
                    flow_name_enercore = wordparts[2] + ' to ' + wordparts[1]
                elif 'Charge' == wordparts[0]:
                    flow_name_enercore = wordparts[1] + ' to ' + wordparts[2]
                operations = operations.rename(
                    {flow_name_FINE: flow_name_enercore})
            else:
                for my in modelyears:

                    ConFac_Check = esM.getComponentAttribute(
                        wordparts[1], 'commodityConversionFactors')[wordparts[0]]

                    if isinstance(ConFac_Check, pd.Series):
                        ConFac_Check = ConFac_Check[0]

                    if ConFac_Check < 0:
                        flow_name_enercore = wordparts[
                            0] + ' to ' + wordparts[1]
                    else:
                        flow_name_enercore = wordparts[
                            1] + ' to ' + wordparts[0]
                    operations.loc[flow_name_enercore, my] = operations.loc[
                        flow_name_FINE, my]
                operations = operations.drop(index=flow_name_FINE)

        elif any(x in flow_name_FINE for x in ['-Snk', '-Dem', '-TSink']):
            Hub = esM.getComponentAttribute(flow_name_FINE, 'commodity')
            flow_name_enercore = Hub + ' to ' + flow_name_FINE
            operations = operations.rename(
                {flow_name_FINE: flow_name_enercore})

        elif any(x in flow_name_FINE for x in ['-Src-', '-WindOnshore', '-WindOffshore', 'P-Trans-Solar', 'P-Trans-DirectAirCapture', '-control', 'Source']):
            Hub = esM.getComponentAttribute(flow_name_FINE, 'commodity')
            flow_name_enercore = flow_name_FINE + ' to ' + Hub
            if flow_name_FINE == Hub:
                operations = operations.drop(index=flow_name_FINE)
            else:
                operations = operations.rename(
                    {flow_name_FINE: flow_name_enercore})

        else:  # all remaining "environments", CO2 Separator,...
            Hub = esM.getComponentAttribute(flow_name_FINE, 'commodity')
            flow_name_enercore = Hub + ' to ' + flow_name_FINE
            operations = operations.rename(
                {flow_name_FINE: flow_name_enercore})

    # turning all values in Sheet positiv
    operations = operations.abs()
    # write transformation-overview.xlsx
    with pd.ExcelWriter(resultfilepath) as writer:
        capacities.to_excel(writer, sheet_name='capacities')
        operations.to_excel(writer, sheet_name='flows')
        emissions.to_excel(writer, sheet_name='CO2')
        totalcosts.to_excel(writer, sheet_name='costs')
