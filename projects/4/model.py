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
from pyspark.ml.regression import LinearRegression


tokenizer = Tokenizer(inputCol="reviewText", outputCol="words")

stop_words = StopWordsRemover.loadDefaultStopWords("english")
swr = StopWordsRemover(inputCol=tokenizer.getOutputCol(), outputCol="words_filtered", stopWords=stop_words)

count_vectorizer = CountVectorizer(inputCol=swr.getOutputCol(), outputCol="word_vector")

assembler = VectorAssembler(inputCols=["verified", 'word_vector'], outputCol="features")

linreg = LinearRegression(featuresCol='features', labelCol="overall", maxIter=20)


pipeline = Pipeline(stages=[
    tokenizer,
    swr,
    count_vectorizer,
    assembler,
    linreg
])

