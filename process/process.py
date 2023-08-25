<<<<<<< HEAD
from Common.reader import parquet_reader 
from pyspark.sql import DataFrame
from Common.saver import save_parquet 
from process.process import (
    create_dataset_join1,
    create_dataset_join2,
    create_dataset_join3
)

from config.config import AppConfig

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
        leftjoin1 = self.create_dataset_join1(maag_master, reac_ref, join_columns, schema)
        leftjoin2 = self.create_dataset_join2(leftjoin1, maag_repa, join_columns, schema)
        result_df = self.create_dataset_join3(leftjoin2, maag_raty, join_column, schema)
        save_parquet(leftjoin1,self.path1)
        save_parquet(leftjoin2,self.path2)
        save_parquet(result_df,self.path3)

    
    
    

    def create_dataset_join1(
        maag_master: DataFrame,
        reac_ref: DataFrame,
        join_columns: list,
        schema: StructType
    ) -> DataFrame:
        """
        This function creates a dataset by performing a left join operation.
        """
        result_df = (
            maag_master.join(reac_ref, on=join_columns, how='left')
            .distinct()
            .select(*schema.fieldNames())
        )
        return result_df

    def create_dataset_join2(
        leftjoin1: DataFrame,
        maag_repa: DataFrame,
        join_columns: list,
        schema: StructType
    ) -> DataFrame:
        """
        Creates a dataset by performing a left join operation.
        """
        result_df = (
            leftjoin1.join(maag_repa, on=join_columns, how='left')
            .distinct()
            .select(*schema.fieldNames())
        )
        return result_df



    def create_dataset_join3(
        leftjoin2: DataFrame,
        maag_raty: DataFrame,
        join_column: str,
        schema: StructType
    ) -> DataFrame:
        """
        Creates a dataset by performing a left join operation.
        """
        result_df = (
            leftjoin2.join(maag_raty, on=join_column, how='left')
            .distinct()
            .select(*schema.fieldNames())
        )
        return result_df
    
    def run_job(**Kwargs:any) -> None : 
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
           





=======
from Common.reader import parquet_reader 
from Common.saver import save_parquet 
from process.process import (
    create_dataset_join1,
    create_dataset_join2,
    create_dataset_join3
)

from config.config import AppConfig 

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
        leftjoin1 = self.create_dataset_join1(maag_master, reac_ref, join_columns, schema)
        leftjoin2 = self.create_dataset_join2(leftjoin1, maag_repa, join_columns, schema)
        result_df = self.create_dataset_join3(leftjoin2, maag_raty, join_column, schema)
        save_parquet(leftjoin1,self.path1)
        save_parquet(leftjoin2,self.path2)
        save_parquet(result_df,self.path3)

    
    
    

    def create_dataset_join1(
        maag_master: DataFrame,
        reac_ref: DataFrame,
        join_columns: list,
        schema: StructType
    ) -> DataFrame:
        """
        This function creates a dataset by performing a left join operation.
        """
        result_df = (
            maag_master.join(reac_ref, on=join_columns, how='left')
            .distinct()
            .select(*schema.fieldNames())
        )
        return result_df

    def create_dataset_join2(
        leftjoin1: DataFrame,
        maag_repa: DataFrame,
        join_columns: list,
        schema: StructType
    ) -> DataFrame:
        """
        Creates a dataset by performing a left join operation.
        """
        result_df = (
            leftjoin1.join(maag_repa, on=join_columns, how='left')
            .distinct()
            .select(*schema.fieldNames())
        )
        return result_df



    def create_dataset_join3(
        leftjoin2: DataFrame,
        maag_raty: DataFrame,
        join_column: str,
        schema: StructType
    ) -> DataFrame:
        """
        Creates a dataset by performing a left join operation.
        """
        result_df = (
            leftjoin2.join(maag_raty, on=join_column, how='left')
            .distinct()
            .select(*schema.fieldNames())
        )
        return result_df
    
    def run_job(**Kwargs:any) -> None : 
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
           





>>>>>>> 3b82bc29147ae04bb8ae3be6c84f134bf5c80c1d
        