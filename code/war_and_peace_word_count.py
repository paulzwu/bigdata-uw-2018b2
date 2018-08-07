'''

Week3 Word count example.

By Paul Wu uwzwu@uw.edu

'''

from __future__ import print_function
import sys
import re

from util import spark, sc
import time
start_time = time.time()

text  =  sc.textFile("data/war_and_peace.txt")
words  =  text.flatMap(lambda  line  :  re.split('\W+', line.lower()))
peace_and_war = text.filter(lambda  line: 'peace' in  re.split('\W+', line.lower()) and 'war' in re.split('\W+', line.lower()))
wordsPairs  =  words.map(lambda  word  :  (word,1))
counts  =  wordsPairs.reduceByKey(lambda  value1,value2  :  value1  +  value2)
counts = counts.sortBy(lambda pair: -pair[1])
counts.foreach(print)

sc.stop()
print("--- %s seconds ---" % (time.time() - start_time))