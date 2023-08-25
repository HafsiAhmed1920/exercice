from dataclasses import dataclass

@dataclass
class AppConfig:
    maag_repa_path: str
    maag_master_path: str
    reac_ref_path: str
    rtpa_ref_path: str
    maag_raty_path: str
    path1: str
    path2: str
    path3: str

app_config = AppConfig(
    maag_repa_path=r"C:\Users\ahafsi\Downloads\tables (1)\tables\maag_repa_rrol_linked",
    maag_master_path=r"C:\Users\ahafsi\Downloads\tables (1)\tables\maag_master_agrem",
    reac_ref_path=r"C:\Users\ahafsi\Downloads\tables (1)\tables\reac_ref_act_type",
    rtpa_ref_path=r"C:\Users\ahafsi\Downloads\tables (1)\tables\rtpa_ref_third_party",
    maag_raty_path=r"C:\Users\ahafsi\Downloads\tables (1)\tables\maag_raty_linked",
    path1=r"C:\Users\ahafsi\Downloads\tables (1)\saved\join1",
    path2=r"C:\Users\ahafsi\Downloads\tables (1)\saved\join2",
    path3=r"C:\Users\ahafsi\Downloads\tables (1)\saved\join3"
)
