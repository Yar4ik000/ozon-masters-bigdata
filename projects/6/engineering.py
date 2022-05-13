#!/opt/conda/envs/dsenv/bin/python
import os
import sys

SPARK_HOME = "/usr/hdp/current/spark2-client"
PYSPARK_PYTHON = "/opt/conda/envs/dsenv/bin/python"
os.environ["PYSPARK_PYTHON"]= PYSPARK_PYTHON
os.environ["SPARK_HOME"] = SPARK_HOME

PYSPARK_HOME = os.path.join(SPARK_HOME, "python/lib")
sys.path.insert(0, os.path.join(PYSPARK_HOME, "py4j-0.10.9.3-src.zip"))
sys.path.insert(0, os.path.join(PYSPARK_HOME, "pyspark.zip"))


from pyspark import SparkConf
from pyspark.sql import SparkSession

conf = SparkConf()

spark = SparkSession.builder.config(conf=conf).appName("Spark ML Intro").getOrCreate()



from pyspark.sql.types import *
from pyspark.ml import Estimator, Transformer
from pyspark.ml.feature import *
from pyspark.ml import Pipeline
from pyspark.sql.types import *

schema = StructType([
    StructField("id", StringType()),
    StructField("label", FloatType()),
    StructField("verified", BooleanType()),
    StructField("vote", IntegerType())
])

df = spark.read.json(sys.argv[2], schema=schema)
imputer = Imputer(strategy='median', inputCol='vote', outputCol='vote')

model = imputer.fit(df)

df = model.transform(df)
df.write().overwrite().parquet(sys.argv[4])

spark.stop()
