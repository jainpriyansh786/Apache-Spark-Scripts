from pyspark import SparkConf, SparkContext

import os

os.environ["PYSPARK_PYTHON"] = "/usr/local/bin/python3"
conf = SparkConf().setAppName("Popular-movie").setMaster("local")
sc = SparkContext(conf=conf)


lines = sc.textFile("ml-100k/u.data")
movies = lines.map(lambda x: (int(x.split()[1]), 1))
movieCounts = movies.reduceByKey(lambda x, y: x + y)

flipped = movieCounts.map( lambda xy: (xy[1],xy[0]) )
sortedMovies = flipped.sortByKey()

results = sortedMovies.collect()

for result in results:
    print(result)
