from pyspark import SparkConf , SparkContext
import os
os.environ["PYSPARK_PYTHON"] = "/usr/local/bin/python3"
conf = SparkConf().setMaster("local").setAppName("Min Temprature")
sc= SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(",")
    stationId = fields[0]
    entryType = fields[2]
    temprature  = float(fields[3])*0.1*(9.0/5.0) + 32.0
    return (stationId,entryType,temprature)

data = sc.textFile("1800.csv")
process_data = data.map(parseLine)
minTempLines = process_data.filter(lambda x : "TMIN" == x[1])
stationTemps = minTempLines.map(lambda x : (x[0],x[2]))
minTempbyStation = stationTemps.reduceByKey(lambda x,y: min(x,y))
results = minTempbyStation.collect()

for i in results:
    print(i[0] + "\t{:.2f}F".format(i[1]))