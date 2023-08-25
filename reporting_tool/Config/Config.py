from dataclasses import dataclass


@dataclass
class AppConfig:
    maag_repa_path: str
    maag_master_path: str
    reac_ref_path: str
    rtpa_ref_path: str
    maag_raty_path: str
    join_columns1: str
    join_columns2: str
    join_columns3: str
    path1: str
    path2: str
    path3: str


AppConfig = AppConfig(
    maag_repa_path=r"C:\Users\ahafsi\Downloads\mytables\tables\maag_repa_rrol_linked",
    maag_master_path=r"C:\Users\ahafsi\Downloads\mytables\tables\maag_master_agrem",
    reac_ref_path=r"C:\Users\ahafsi\Downloads\mytables\tables\reac_ref_act_type",
    rtpa_ref_path=r"C:\Users\ahafsi\Downloads\mytables\tables\rtpa_ref_third_party",
    maag_raty_path=r"C:\Users\ahafsi\Downloads\mytables\tables\maag_raty_linked",
    join_columns1=["c_act_type", "n_applic_infq"],
    join_columns2=['N_APPLIC_INFQ', 'C_MAST_AGREM_REFER'],
    join_columns3=["c_mast_agrem_refer"],
    path1=r"C:\Users\ahafsi\Downloads\mytables\saved\join1",
    path2=r"C:\Users\ahafsi\Downloads\mytables\saved\join2",
    path3=r"C:\Users\ahafsi\Downloads\mytables\saved\join3"
)
