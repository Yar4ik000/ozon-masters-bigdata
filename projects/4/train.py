from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
spark.sparkContext.setLogLevel('WARN')

from model import pipeline
import sys


schema = StructType([
    StructField("overall", FloatType()),
    StructField("verified", BooleanType()),
    StructField("reviewText", StringType())
])

dataset = spark.read.json(sys.argv[1], schema=schema)

pipeline_model = pipeline.fit(dataset)
pipeline_model.write().overwrite().save(sys.argv[2])

