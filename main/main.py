from Common.reader import parquet_reader 
from Common.saver import parquet_saver 
from process.process import left_join 
#main 
maag_repa = parquet_reader(AppConfig.maag_repa_path)
maag_master = parquet_reader(AppConfig.maag_master_path)
rtpa_ref    = parquet_reader(AppConfig.rtpa_ref_path)
reac_ref  = parquet_reader(AppConfig.reac_ref_path)
maag_raty = parquet_reader(AppConfig.maag_raty_path)



leftjoin1 = left_join(maag_master,reac_ref,['c_act_type', 'n_applic_infq']) 
leftjoin2 = left_join(leftjoin1 , maag_repa , ['N_APPLIC_INFQ' ,'C_MAST_AGREM_REFER'])
leftjoin3 = left_join(leftjoin2 , maag_raty , 'c_mast_agrem_refer')

save_parquet(leftjoin1, AppConfig.path1)
save_parquet(leftjoin2, AppConfig.path2)
save_parquet(leftjoin3, AppConfig.path3)