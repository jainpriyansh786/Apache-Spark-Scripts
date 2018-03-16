from pyspark import SparkConf , SparkContext
import os

os.environ["PYSPARK_PYTHON"] = "/usr/local/bin/python3"

conf = SparkConf().setAppName("Popular-superhero").setMaster("local")
sc = SparkContext(conf=conf)






def parseName(line):
    datalines = line.split('\"')
    return (int(datalines[0]),str(datalines[1]))


def parseGraph(line):

    datalines = line.split()
    return (int(datalines[0]), len(datalines)-1)



superGraph = sc.textFile("Marvel-Graph.txt").map(parseGraph).reduceByKey(lambda x,y:x+y )

heroNames = sc.textFile("Marvel-Names.txt").map(parseName)

mostPopular = superGraph.max(key = lambda x : x[1])

print("The most popular SuperHero is" + str(heroNames.lookup(mostPopular[0])) )


