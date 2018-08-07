'''

By Paul Wu uwzwu@uw.edu

'''

from __future__ import print_function
from util import sc


lines = sc.textFile("../data/airport-codes.csv")
print ("partition size", lines.getNumPartitions())
#If this is not done, it will get a memory error.
lines = lines.repartition(200)
california = lines.filter(lambda line: "US-CA" in line)
print(california.take(10))
norcal = california.filter(lambda line: float(line.split(',')[4].strip('"')) > 37)
codes = norcal.map(lambda line: line.split(',')[0])
print("count", codes.count())
sc.stop()
