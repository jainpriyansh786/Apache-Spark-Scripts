from pyspark import SparkConf, SparkContext
import collections
import os
#os.environ["SPARK_HOME"] = "/usr/local/Cellar/apache-spark/1.5.1/"
os.environ["PYSPARK_PYTHON"]="/usr/local/bin/python3"
conf = SparkConf().setAppName("RatingsHistogram").setMaster("local")
sc = SparkContext(conf = conf)

lines = sc.textFile("ml-100k/u.data")
ratings = lines.map(lambda x: x.split()[2])
result = ratings.countByValue()
sortedResults = collections.OrderedDict(sorted(result.items()))
for key, value in sortedResults.items():
    print("%s %i" % (key, value))
