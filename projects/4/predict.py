from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
spark.sparkContext.setLogLevel('WARN')

from pyspark.ml import Pipeline, PipelineModel
import sys

model = PipelineModel.load(sys.argv[1])


schema = StructType([
    StructField("overall", FloatType()),
    StructField("verified", BooleanType()),
    StructField("reviewText", StringType())
])
df_test = spark.read.json(sys.argv[2], schema=schema)
predictions = model.transform(df_test)


only_preds = predictions.select('prediction')
only_preds.write.csv(sys.argv[3])

