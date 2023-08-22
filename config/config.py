# Config
from dataclasses import dataclass

@dataclass
class AppConfig:
   
    maag_repa_path : str
    maag_master_path : str
    reac_ref_path : str
    rtpa_ref_path : str
    maag_raty_path : str
    path1 : str
    path2  : str
    path3  : str 





    app_config = AppConfig(
    maag_repa_path = "/FileStore/tables/tables (1)/maag_repa_rrol_linked/maag_repa_rrol_linked"
    maag_master_path = "/FileStore/tables/tables (1)/tables/maag_master_agrem"
    reac_ref_path = "/FileStore/tables/tables (1)/tables/reac_ref_act_type"
    rtpa_ref_path = "/FileStore/tables/tables (1)/tables/rtpa_ref_third_party"
    maag_raty_path = "/FileStore/tables/tables (1)/tables/maag_raty_linked"
    path1  = " paath here "
    path2  = "path/to/save"
    path3  = "path/to/save" )