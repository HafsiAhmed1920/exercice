from pyspark.sql import SparkSession


class SparkSessionHandler:
    def __init__(self, app_name: str, master: str = "local[*]"):
        self.app_name = app_name
        self.master = master
        self.spark = None

    def start(self):
        if not self.spark:
            self.spark = SparkSession.builder \
                .appName(self.app_name) \
                .master(self.master) \
                .getOrCreate()
        return self.spark

    def stop(self):
        if self.spark:
            self.spark.stop()
            self.spark = None

    def get_spark(self):
        if not self.spark:
            raise Exception("failed to start spark.")
        return self.spark
