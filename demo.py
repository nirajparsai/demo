import json
from pyspark.dbutils import DBUtils
from pyspark.sql import Row


def rddprint(filename,spark):
    dbutils = DBUtils(spark)
    list = [('Niraj',31),('Jay',25),('Harish',26),('Mayur',29)]
    rdd = sc.parallelize(list)
    df = spark.createDataFrame(rdd)
    df.write.parquet("/tmp/"+filename+".csv")
    display(df)