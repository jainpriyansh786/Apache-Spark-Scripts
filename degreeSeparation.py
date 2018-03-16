from pyspark import SparkConf , SparkContext
import os

os.environ["PYSPARK_PYTHON"] = "/usr/local/bin/python3"

conf = SparkConf().setAppName("Degree-Sepration").setMaster("local")
sc = SparkContext(conf=conf)
hitCounter = sc.accumulator(0)

startId = 5306;
targetId = 10;
def changeToBfs(line):
    data = line.split()
    heroId = int(data[0])
    connections = list(map(lambda x : int(x) , data[1:]))

    color = "white"

    distance = 9999

    if heroId == startId :
        distance = 0
        color = "grey"

    return (heroId , (connections,color,distance))


def createRdd():
    inputRdd = sc.textFile("Marvel-Graph.txt").map(changeToBfs)
    return inputRdd

def traverse(node):
    characterId = node[0]
    fields = node[1]
    connection = fields[0]
    color = fields[1]
    distance = fields[2]


    result = []

    if color == "grey":
        for i in connection :
            new_characterId = i
            new_connection = []
            new_color = "white"
            new_distance = distance + 1
            if (targetId == i):
                hitCounter.add(1)
            newNode = (new_characterId , (new_connection,new_color,new_distance))
            result.append(newNode)
        color = 'black'

    result.append((characterId,(connection,color,distance)))

    return result

def bfsReduce(data1, data2):
    edges1 = data1[0]
    edges2 = data2[0]
    distance1 = data1[2]
    distance2 = data2[2]
    color1 = data1[1]
    color2 = data2[1]

    distance = 9999
    color = color1
    edges = []

    # See if one is the original node with its connections.
    # If so preserve them.
    if (len(edges1) > 0):
        edges.extend(edges1)
    if (len(edges2) > 0):
        edges.extend(edges2)

    # Preserve minimum distance


    if (distance1 < distance):
        distance = distance1

    if (distance2 < distance):
        distance = distance2

    # Preserve darkest color
    if (color1 == 'WHITE' and (color2 == 'GRAY' or color2 == 'BLACK')):
        color = color2

    if (color1 == 'GRAY' and color2 == 'BLACK'):
        color = color2

    if (color2 == 'WHITE' and (color1 == 'GRAY' or color1 == 'BLACK')):
        color = color1

    if (color2 == 'GRAY' and color1 == 'BLACK'):
        color = color1

    return (edges, color, distance)


mainRdd = createRdd()

for i in range(1,10):
    traveredRdd = mainRdd.flatMap(traverse)

    print("Processing " + str(traveredRdd.count()) + " values.")

    if hitCounter.value > 0 :
        print("found after" , hitCounter.value)
        break

    mainRdd = traveredRdd.reduceByKey(bfsReduce)





