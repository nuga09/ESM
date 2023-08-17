import FINE as fn
import pandas as pd


def update_parameter(component, esM, updated_parameter_dict, system_with_ee_restriction=False):
    # get dict to update parameter
    if isinstance(component, fn.Source):
        # TODO as long as sharedCapacityExpansion is not on dev branch this differentiation is required
        if system_with_ee_restriction:
            param_list = [
                "name", "commodity", "hasCapacityVariable", "operationRateMax",
                "operationRateFix", "capacityFix", "capacityMax", "capacityMin",
                "investPerCapacity", "opexPerCapacity", "opexPerOperation",
                "commodityCost", "commodityRevenue", "interestRate",
                "economicLifetime", "QPcostScale", "technicalLifetime",
                "sharedExpansionID", "sharedCapacityExpansionMin",
                "sharedCapacityExpansionMax"]
        else:
            param_list = [
                "name", "commodity", "hasCapacityVariable", "operationRateMax",
                "operationRateFix", "capacityFix", "capacityMax", "capacityMin",
                "investPerCapacity", "opexPerCapacity", "opexPerOperation",
                "commodityCost", "commodityRevenue", "interestRate",
                "economicLifetime", "QPcostScale", "technicalLifetime"]
    elif isinstance(component, fn.Sink):
        param_list = [
            "name", "commodity", "hasCapacityVariable", "operationRateMax",
            "operationRateFix", "capacityFix", "capacityMax", "capacityMin",
            "investPerCapacity", "opexPerCapacity", "opexPerOperation",
            "commodityCost", "commodityRevenue", "interestRate",
            "economicLifetime", "QPcostScale", "technicalLifetime", ]
    elif isinstance(component, fn.Conversion):
        param_list = [
            "name", "physicalUnit", "commodityConversionFactors",
            "hasCapacityVariable", "capacityMax", "capacityMin", "capacityFix",
            "investPerCapacity", "opexPerOperation", "opexPerCapacity",
            "opexPerCapacity", "economicLifetime", "technicalLifetime",
            "QPcostScale",  "interestRate", "operationRateFix"]
    elif isinstance(component, fn.Storage):
        param_list = [
            "name", "commodity", "hasCapacityVariable", "capacityFix",
            "capacityMax", "capacityMin", "investPerCapacity",
            "opexPerCapacity", "opexPerChargeOperation", "chargeEfficiency",
            "dischargeEfficiency", "selfDischarge", "interestRate",
            "economicLifetime"]
    else:
        raise ValueError()

    # check if the updated parameter exists in the parameter of component
    if any([x for x in updated_parameter_dict.keys() if x not in param_list]):
        _missing = [x for x in updated_parameter_dict.keys()
                    if x not in param_list]
        raise ValueError(
            f"Passed parameter '{_missing}' to update '{component.name}' not in parameter list")

    update_component_parameter = {}
    for param in param_list:
        if param in updated_parameter_dict.keys():
            update_component_parameter[param] = updated_parameter_dict[param]
        else:
            update_component_parameter[param] = getattr(component, param)

    # for capacity get values out of the dict
    for param in ["capacityFix", "capacityMax", "capacityMin", "investPerCapacity", "opexPerOperation", "opexPerCapacity", "economicLifetime", "technicalLifetime", "interestRate", "QPcostScale"]:
        if (param == "QPcostScale" or param == "opexPerOperation" or param == "technicalLifetime") and param not in update_component_parameter.keys():
            continue
        if isinstance(update_component_parameter[param], dict):
            if len(update_component_parameter[param]) > 1:
                raise ValueError()
            region = list(update_component_parameter[param].keys())[0]
            update_component_parameter[param] =\
                update_component_parameter[param][region]
        if isinstance(update_component_parameter[param], pd.Series):
            if len(update_component_parameter[param]) == 1:
                update_component_parameter[param] =\
                    update_component_parameter[param].values[0]

    # capacity max cannot be int
    if update_component_parameter["capacityMax"] is not None:
        update_component_parameter["capacityMax"] =\
            float(update_component_parameter["capacityMax"])
    if update_component_parameter["capacityFix"] is not None:
        update_component_parameter["capacityFix"] =\
            float(update_component_parameter["capacityFix"])
    if update_component_parameter["capacityMin"] is not None:
        update_component_parameter["capacityMin"] =\
            float(update_component_parameter["capacityMin"])
    # update parameter
    # for sources, sign comparison is needed, otherwises also sink chosen
    if isinstance(component, fn.Source) and component.sign == 1:
        addSource(esM, sourceParameter=update_component_parameter,system_with_ee_restriction=system_with_ee_restriction)
    elif isinstance(component, fn.Sink):
        addSink(esM, sinkParameter=update_component_parameter)
    elif isinstance(component, fn.Conversion):
        esM = addConversion(esM, update_component_parameter)
    elif isinstance(component, fn.Storage):
        esM = addStorage(esM, storageParameter=update_component_parameter)
    else:
        raise ValueError(
            "Unknown component type for "+component.name)
    return esM


def addSource(esM, sourceParameter, system_with_ee_restriction):
    if system_with_ee_restriction:
        if sourceParameter["QPcostScale"] is None or (sourceParameter["capacityMax"] is None and sourceParameter["capacityMin"] is None):
            esM.add(
                fn.Source(
                    esM=esM,
                    name=sourceParameter["name"],
                    commodity=sourceParameter["commodity"],
                    hasCapacityVariable=sourceParameter["hasCapacityVariable"],
                    operationRateMax=sourceParameter["operationRateMax"],
                    operationRateFix=sourceParameter["operationRateFix"],
                    capacityFix=sourceParameter["capacityFix"],
                    capacityMax=sourceParameter["capacityMax"],
                    capacityMin=sourceParameter["capacityMin"],
                    investPerCapacity=sourceParameter["investPerCapacity"],
                    opexPerCapacity=sourceParameter["opexPerCapacity"],
                    opexPerOperation=sourceParameter["opexPerOperation"],
                    commodityCost=sourceParameter["commodityCost"],
                    commodityRevenue=sourceParameter["commodityRevenue"],
                    interestRate=sourceParameter["interestRate"],
                    economicLifetime=sourceParameter["economicLifetime"],
                    technicalLifetime=sourceParameter["technicalLifetime"],
                    sharedCapacityExpansionMax=sourceParameter["sharedCapacityExpansionMax"],
                    sharedCapacityExpansionMin=sourceParameter["sharedCapacityExpansionMin"],
                    sharedExpansionID=sourceParameter["sharedExpansionID"]))
        else:
            esM.add(
                fn.Source(
                    esM=esM,
                    name=sourceParameter["name"],
                    commodity=sourceParameter["commodity"],
                    hasCapacityVariable=sourceParameter["hasCapacityVariable"],
                    operationRateMax=sourceParameter["operationRateMax"],
                    operationRateFix=sourceParameter["operationRateFix"],
                    capacityFix=sourceParameter["capacityFix"],
                    capacityMax=sourceParameter["capacityMax"],
                    capacityMin=sourceParameter["capacityMin"],
                    investPerCapacity=sourceParameter["investPerCapacity"],
                    opexPerCapacity=sourceParameter["opexPerCapacity"],
                    opexPerOperation=sourceParameter["opexPerOperation"],
                    commodityCost=sourceParameter["commodityCost"],
                    commodityRevenue=sourceParameter["commodityRevenue"],
                    interestRate=sourceParameter["interestRate"],
                    economicLifetime=sourceParameter["economicLifetime"],
                    QPcostScale=sourceParameter["QPcostScale"],
                    technicalLifetime=sourceParameter["technicalLifetime"],
                    sharedCapacityExpansionMax=sourceParameter["sharedCapacityExpansionMax"],
                    sharedCapacityExpansionMin=sourceParameter["sharedCapacityExpansionMin"],
                    sharedExpansionID=sourceParameter["sharedExpansionID"]))
    else:
        if sourceParameter["QPcostScale"] is None or (sourceParameter["capacityMax"] is None and sourceParameter["capacityMin"] is None):
            esM.add(
                fn.Source(
                    esM=esM,
                    name=sourceParameter["name"],
                    commodity=sourceParameter["commodity"],
                    hasCapacityVariable=sourceParameter["hasCapacityVariable"],
                    operationRateMax=sourceParameter["operationRateMax"],
                    operationRateFix=sourceParameter["operationRateFix"],
                    capacityFix=sourceParameter["capacityFix"],
                    capacityMax=sourceParameter["capacityMax"],
                    capacityMin=sourceParameter["capacityMin"],
                    investPerCapacity=sourceParameter["investPerCapacity"],
                    opexPerCapacity=sourceParameter["opexPerCapacity"],
                    opexPerOperation=sourceParameter["opexPerOperation"],
                    commodityCost=sourceParameter["commodityCost"],
                    commodityRevenue=sourceParameter["commodityRevenue"],
                    interestRate=sourceParameter["interestRate"],
                    economicLifetime=sourceParameter["economicLifetime"],
                    technicalLifetime=sourceParameter["technicalLifetime"],))
        else:
            esM.add(
                fn.Source(
                    esM=esM,
                    name=sourceParameter["name"],
                    commodity=sourceParameter["commodity"],
                    hasCapacityVariable=sourceParameter["hasCapacityVariable"],
                    operationRateMax=sourceParameter["operationRateMax"],
                    operationRateFix=sourceParameter["operationRateFix"],
                    capacityFix=sourceParameter["capacityFix"],
                    capacityMax=sourceParameter["capacityMax"],
                    capacityMin=sourceParameter["capacityMin"],
                    investPerCapacity=sourceParameter["investPerCapacity"],
                    opexPerCapacity=sourceParameter["opexPerCapacity"],
                    opexPerOperation=sourceParameter["opexPerOperation"],
                    commodityCost=sourceParameter["commodityCost"],
                    commodityRevenue=sourceParameter["commodityRevenue"],
                    interestRate=sourceParameter["interestRate"],
                    economicLifetime=sourceParameter["economicLifetime"],
                    QPcostScale=sourceParameter["QPcostScale"],
                    technicalLifetime=sourceParameter["technicalLifetime"],))
    return esM


def addSink(esM, sinkParameter):
    if sinkParameter["QPcostScale"] is None:
        esM.add(
            fn.Sink(
                esM=esM,
                name=sinkParameter["name"],
                commodity=sinkParameter["commodity"],
                hasCapacityVariable=sinkParameter["hasCapacityVariable"],
                operationRateMax=sinkParameter["operationRateMax"],
                operationRateFix=sinkParameter["operationRateFix"],
                capacityFix=sinkParameter["capacityFix"],
                capacityMax=sinkParameter["capacityMax"],
                capacityMin=sinkParameter["capacityMin"],
                investPerCapacity=sinkParameter["investPerCapacity"],
                opexPerCapacity=sinkParameter["opexPerCapacity"],
                opexPerOperation=sinkParameter["opexPerOperation"],
                commodityCost=sinkParameter["commodityCost"],
                commodityRevenue=sinkParameter["commodityRevenue"],
                interestRate=sinkParameter["interestRate"],
                economicLifetime=sinkParameter["economicLifetime"],
                technicalLifetime=sinkParameter["technicalLifetime"]))

    else:
        esM.add(
            fn.Sink(
                esM=esM,
                name=sinkParameter["name"],
                commodity=sinkParameter["commodity"],
                hasCapacityVariable=sinkParameter["hasCapacityVariable"],
                operationRateMax=sinkParameter["operationRateMax"],
                operationRateFix=sinkParameter["operationRateFix"],
                capacityFix=sinkParameter["capacityFix"],
                capacityMax=sinkParameter["capacityMax"],
                capacityMin=sinkParameter["capacityMin"],
                investPerCapacity=sinkParameter["investPerCapacity"],
                opexPerCapacity=sinkParameter["opexPerCapacity"],
                opexPerOperation=sinkParameter["opexPerOperation"],
                commodityCost=sinkParameter["commodityCost"],
                commodityRevenue=sinkParameter["commodityRevenue"],
                interestRate=sinkParameter["interestRate"],
                economicLifetime=sinkParameter["economicLifetime"],
                QPcostScale=sinkParameter["QPcostScale"],
                technicalLifetime=sinkParameter["technicalLifetime"]))
    return esM


def addConversion(esM, conversionParameter):
    if conversionParameter["QPcostScale"] is not None and conversionParameter["QPcostScale"] != 0 and (conversionParameter["capacityMax"] is not None and conversionParameter["capacityMin"] is not None):
        esM.add(
            fn.Conversion(
                esM=esM,
                name=conversionParameter["name"],
                physicalUnit=conversionParameter["physicalUnit"],
                commodityConversionFactors=conversionParameter["commodityConversionFactors"],
                hasCapacityVariable=conversionParameter["hasCapacityVariable"],
                capacityMax=conversionParameter["capacityMax"],
                capacityMin=conversionParameter["capacityMin"],
                capacityFix=conversionParameter["capacityFix"],
                investPerCapacity=conversionParameter["investPerCapacity"],
                opexPerOperation=conversionParameter["opexPerOperation"],
                opexPerCapacity=conversionParameter["opexPerCapacity"],
                economicLifetime=conversionParameter["economicLifetime"],
                technicalLifetime=conversionParameter["technicalLifetime"],
                QPcostScale=conversionParameter["QPcostScale"],
                operationRateFix=conversionParameter["operationRateFix"],
                interestRate=conversionParameter["interestRate"]))
    else:
        esM.add(
            fn.Conversion(
                esM=esM,
                name=conversionParameter["name"],
                physicalUnit=conversionParameter["physicalUnit"],
                commodityConversionFactors=conversionParameter["commodityConversionFactors"],
                hasCapacityVariable=conversionParameter["hasCapacityVariable"],
                capacityMax=conversionParameter["capacityMax"],
                capacityMin=conversionParameter["capacityMin"],
                capacityFix=conversionParameter["capacityFix"],
                investPerCapacity=conversionParameter["investPerCapacity"],
                opexPerOperation=conversionParameter["opexPerOperation"],
                opexPerCapacity=conversionParameter["opexPerCapacity"],
                economicLifetime=conversionParameter["economicLifetime"],
                technicalLifetime=conversionParameter["technicalLifetime"],
                operationRateFix=conversionParameter["operationRateFix"],
                interestRate=conversionParameter["interestRate"]))
    return esM


def addStorage(esM, storageParameter):
    esM.add(
        fn.Storage(
            esM=esM,
            name=storageParameter["name"],
            hasCapacityVariable=storageParameter["hasCapacityVariable"],
            commodity=storageParameter["commodity"],
            capacityFix=storageParameter["capacityFix"],
            capacityMax=storageParameter["capacityMax"],
            capacityMin=storageParameter["capacityMin"],
            investPerCapacity=storageParameter["investPerCapacity"],
            opexPerCapacity=storageParameter["opexPerCapacity"],
            opexPerChargeOperation=storageParameter["opexPerChargeOperation"],
            chargeEfficiency=storageParameter["chargeEfficiency"],
            dischargeEfficiency=storageParameter["dischargeEfficiency"],
            selfDischarge=storageParameter["selfDischarge"],
            interestRate=storageParameter["interestRate"],
            economicLifetime=storageParameter["economicLifetime"]))
    return esM
