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
from pyspark.sql.types import *


conf = SparkConf()

spark = SparkSession.builder.config(conf=conf).appName("Spark SQL").getOrCreate()


twitter_schema = StructType(fields=[
    StructField("user_id", IntegerType()),
    StructField("follower_id", IntegerType())
])

twitter = spark.read\
               .schema(twitter_schema)\
               .format('csv')\
               .option('sep', '\t')\
               .load(sys.argv[3])

from collections import deque, defaultdict


vertexs = twitter.collect()

graph = defaultdict(list)

for user, follower in vertexs:
    graph[follower].append(user)


def BFS(start, end, max_depth=10):
    route = ''
    opened = set()
    queue = deque()
    min_ans = max_depth + 10
    queue.append([start, 0, str(start)])
    opened.add(start)
    while queue:
        elem = queue.popleft()
        if elem[0] == end:
            if elem[1] < min_ans:
                route = elem[2]
                min_ans = elem[1]
            continue
        neighbours = graph[elem[0]]
        for neigh in neighbours:
            if neigh not in opened and elem[1] + 1 < max_depth:
                queue.append([neigh, elem[1] + 1, elem[2] + ',' + str(neigh)])
                opened.add(neigh)
    return route

import csv
import subprocess


subprocess.run(['hdfs', 'dfs', '-mkdir', sys.argv[4]])

with open(sys.argv[4], 'w') as f:
    writer = csv.writer(f)
    writer.writerow(BFS(int(sys.argv[1]), int(sys.argv[2])))

