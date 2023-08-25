""" taliking about the code in here """

from Config.Config import AppConfig
from Common.reader import parquet_reader 
from pyspark.sql import DataFrame 
from pyspark.sql.types import StructType
from Common.saver import save_parquet 


class CodeYJob:
    def __init__(
        self,
        maag_repa_path: str,
        maag_master_path: str,
        rtpa_ref_path: str,
        reac_ref_path: str,
        maag_raty_path: str,
        path1: str,
        path2: str,
        path3: str
    ) -> None:
        
        self.maag_repa_path = maag_repa_path
        self.maag_master_path = maag_master_path
        self.rtpa_ref_path = rtpa_ref_path
        self.reac_ref_path = reac_ref_path
        self.maag_raty_path = maag_raty_path
        self.path1 = path1
        self.path2 = path2
        self.path3 = path3

    def run(self) -> None:
        
        maag_repa = parquet_reader(self.maag_repa_path)
        maag_master = parquet_reader(self.maag_master_path)
        rtpa_ref = parquet_reader(self.rtpa_ref_path)
        reac_ref = parquet_reader(self.reac_ref_path)
        maag_raty = parquet_reader(self.maag_raty_path)
         # Perform join operations to create datasets
        leftjoin1 = self.create_dataset_join1(maag_master, reac_ref, AppConfig.join_columns1)
        leftjoin2 = self.create_dataset_join2(leftjoin1, maag_repa, AppConfig.join_columns2)
        result_df = self.create_dataset_join3(leftjoin2, maag_raty, AppConfig.join_columns3)
        save_parquet(leftjoin1,self.path1)
        save_parquet(leftjoin2,self.path2)
        save_parquet(result_df,self.path3)

    
    
    

    def create_dataset_join1(
        self,
        maag_master: DataFrame,
        reac_ref: DataFrame,
        join_columns: list
    ) -> DataFrame:
        """
        This function creates a dataset by performing a left join operation.
        """
        result_df = (
            maag_master.join(reac_ref, on=join_columns, how='left')
            .distinct()
        )
        return result_df

    def create_dataset_join2(
        self,
        leftjoin1: DataFrame,
        maag_repa: DataFrame,
        join_columns: list
    ) -> DataFrame:
        """
        Creates a dataset by performing a left join operation.
        """
        result_df = (
            leftjoin1.join(maag_repa, on=join_columns, how='left')
            .distinct()
        )
        return result_df



    def create_dataset_join3(
        self,
        leftjoin2: DataFrame,
        maag_raty: DataFrame,
        join_column: str
    ) -> DataFrame:
        """
        Creates a dataset by performing a left join operation.
        """
        result_df = (
            leftjoin2.join(maag_raty, on=join_column, how='left')
            .distinct()
        )
        return result_df
    """ this function run the job"""
    def run_job(self, **Kwargs: any) -> None: 
        maag_repa = AppConfig.maag_repa_path
        maag_master = AppConfig.maag_master_path
        rtpa_ref = AppConfig.rtpa_ref_path
        reac_ref = AppConfig.reac_ref_path
        maag_raty = AppConfig.maag_raty_path
        output_path_1 = AppConfig.path1
        output_path_2 = AppConfig.path2
        output_path_3 = AppConfig.path3 
        
        job: CodeYJob = CodeYJob(
                    maag_repa,
                    maag_master,
                    rtpa_ref,
                    reac_ref,
                    maag_raty,
                    output_path_1,
                    output_path_2,
                    output_path_3
                       )
        job.run()
           





        