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



from pyspark.ml import Estimator, Transformer
from pyspark.ml.feature import *
from pyspark.ml import Pipeline
from pyspark.sql.types import *
from pyspark.sql import functions as F
import pandas as pd
import numpy as np


schema = StructType([
    StructField("id", StringType()),
    StructField("verified", BooleanType()),
    StructField("vote", StringType())
])

df = spark.read.parquet(sys.argv[1], schema=schema)

model = joblib.load(sys.argv[3])
logreg = spark.sparkContext.broadcast(model)

@F.pandas_udf(IntegerType())
def predict(*columns):
    preds = logreg.value.predict_proba(pd.concat(columns, axis=1))[:, 1]
    return pd.Series(preds)

preds = df.withColumn('prediction', predict('verified', 'vote'))

preds.select('id', 'prediction').write().overwrite().save(sys.argv[2], header='false', format='csv')

spark.stop()
