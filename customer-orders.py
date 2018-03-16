from pyspark import SparkContext , SparkConf

import os

os.environ["PYSPARK_PYTHON"]="/usr/local/bin/python3"
conf = SparkConf().setMaster("local").setAppName("friend-by-age")

sc = SparkContext(conf= conf)


data = sc.textFile("customer-orders.csv")
lines = data.map(lambda x : x.split(",")).map(lambda x : (int(x[0]) , float(x[2]))).reduceByKey(lambda x,y:x+y).sortBy(lambda x: x[1]).collect()

for i in lines:
    print(str(i[0]) + "\t" +str(i[1]))
