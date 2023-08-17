import pandas as pd
import os


def combine_transformation_overview_files(path, files_list,qp_approach=True):
    save_path = os.path.join(
        path, "Transformation_overview_compare.xlsx")
    if qp_approach is True:
        sheet_names=["capacities", "flows", "CO2", "costs","costsQP"]
    else:
        sheet_names=["capacities", "flows", "CO2", "costs"]
    for sheet_name in sheet_names:
        new_sheet = pd.DataFrame()
        for file in files_list:
            case_name = file.split("_")[-1].split(".")[0]
            file_path = os.path.join(path, file)
            sheet_df = pd.read_excel(
                file_path, sheet_name=sheet_name, index_col=0)
            new_sheet = pd.concat([new_sheet, sheet_df["Target Year"]], axis=1).rename(
                columns={"Target Year": case_name})
        new_sheet.fillna(0)
        if not os.path.isfile(save_path):
            with pd.ExcelWriter(save_path) as writer:
                new_sheet.to_excel(
                    writer, sheet_name=sheet_name)
        else:
            if sheet_name in pd.ExcelFile(save_path).sheet_names:
                with pd.ExcelWriter(save_path, engine='openpyxl') as writer:
                    new_sheet.to_excel(writer, sheet_name=sheet_name)
            else:
                with pd.ExcelWriter(save_path, engine='openpyxl', mode='a') as writer:
                    new_sheet.to_excel(writer, sheet_name=sheet_name)


if __name__ == "__main__":
    # path = r"R:\git\Nestor_THG0\NESTOR\Modell\Results\mobility\TransformationDateien"
    # files = ["transformation_overview_base.xlsx",
    #          "transformation_overview_BEV.xlsx",
    #          "transformation_overview_FCEV.xlsx",
    #          # "transformation_overview_BEVFCEV.xlsx",
    #          "transformation_overview_BETr.xlsx",
    #          "transformation_overview_BERoad.xlsx",
    #          "transformation_overview_FCRoad.xlsx"]

    # files = ["transformation_overview_QP_base.xlsx",
    #          "transformation_overview_QP_BEV.xlsx",
    #          "transformation_overview_QP_BEVnewts.xlsx",
    #          # "transformation_overview_QP_FCEV.xlsx",
    #          # "transformation_overview_BEVFCEV.xlsx",
    #          # "transformation_overview_QP_BETr.xlsx",
    #          # "transformation_overview_QP_BE50FC50road.xlsx",
    #          # "transformation_overview_QP_BERoad.xlsx",
    #          # "transformation_overview_QP_FCRoad.xlsx"
    #          ]

    # files = ["transformation_overview_QP_base.xlsx",
    #          "transformation_overview_QP_BERoad.xlsx",
    #          "transformation_overview_QP_BETr.xlsx",
    #          "transformation_overview_QP_BEV.xlsx",
    #          "transformation_overview_QP_BE50FC50road.xlsx",
    #          "transformation_overview_QP_FCEV.xlsx",
    #          "transformation_overview_QP_FCRoad.xlsx"]
    path=r"R:\git\nestor\Modell\Results\Nestor_compare"
    files = ["transformation_overview_new.xlsx",
             "transformation_overview_thomas.xlsx"]
    combine_transformation_overview_files(path, files)
