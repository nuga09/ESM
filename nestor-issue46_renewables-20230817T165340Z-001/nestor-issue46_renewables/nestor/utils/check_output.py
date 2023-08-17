import pandas as pd


def _round(data):
    data = round(data, 4)
    return data


def check_output(databasepath, resultfilepath, historicaldatapath, targetyear):
    # check if upper bounds are kept
    check_upper_bounds(databasepath, resultfilepath,
                       historicaldatapath, targetyear)


def check_upper_bounds(databasepath, resultfilepath, historicaldatapath,
                       targetyear):
    raw_transformers = pd.read_excel(
        databasepath, sheet_name='Transformers', index_col="name")
    raw_sources = pd.read_excel(
        databasepath, sheet_name='Sources', index_col=0)
    raw_heatpumps = pd.read_excel(
        databasepath, sheet_name='HeatPumps', index_col=0)
    raw_storages = pd.read_excel(
        databasepath, sheet_name='Storages', index_col=0)
    raw_sink = pd.read_excel(
        databasepath, sheet_name='Sinks', index_col=0)
    historicalcapacity = pd.read_excel(
        historicaldatapath, index_col="Year")
    result_capacities = pd.read_excel(
        resultfilepath, sheet_name="capacities", index_col=0)

    raw_ubs = pd.concat([
        raw_transformers["ub"],
        raw_sources["ub"],
        raw_heatpumps["ub"],
        raw_storages["ub"],
        raw_sink["ub"]], axis=0)

    for comp_name in result_capacities.index:
        for year in result_capacities.columns:
            if year == 'target year':
                _year_historical_data = targetyear
            else:
                _year_historical_data = year

            # components not to check
            if ("IDemand" in comp_name) or ("AEDemand" in comp_name):
                continue
            if comp_name.endswith("Virt") or comp_name == "CO2Environment":
                continue

            if comp_name in raw_ubs.index:
                # result capacity of component
                comp_result_capacity = _round(
                    result_capacities.loc[comp_name, year])

                # raw ub of component
                raw_ub = _round(raw_ubs.loc[comp_name])

                # compare results with raw ub and historical data
                ub_error_raise = False
                if (comp_result_capacity > raw_ub):
                    if comp_name in historicalcapacity.columns:
                        historical_cap_of_year = _round(historicalcapacity.loc[
                            _year_historical_data, comp_name])
                        if comp_result_capacity > historical_cap_of_year:
                            ub_error_raise = True
                        else:
                            ub_error_raise = False
                    else:
                        pass  # results are fine

                #  raise error
                if ub_error_raise:
                    print("\nUpper bound exceeded for " +
                          "'{}' in year '{}'.".format(comp_name, year))
                    print("Result: {}".format(comp_result_capacity))
                    print("Upper bound: {}".format(raw_ub))
                    if comp_name in historicalcapacity.columns:
                        print("Historical Capacity for year: {}".format(
                            historical_cap_of_year))

            else:
                print("Mapping of '{}' ".format(comp_name) +
                      "not possible for checking ub in check_output.py")
