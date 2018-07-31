'''

Word count problem.

By Paul Wu uwzwu@uw.edu

'''


from __future__ import print_function
import re

from util import sc

#Read files...can be from hdfs.
text_file = sc.textFile("../data/words*.txt")

#Transform the rdd into key-value pair (word, count) and use reduce alg.
counts = text_file.flatMap(lambda line: re.split('\W+', line)) \
             .map(lambda word: (word.lower(), 1)) \
             .reduceByKey(lambda a, b: a + b)

#print
counts.foreach(print)

#sort
# this 5
# is 6

counts = counts.sortBy(lambda pair: -pair[1])

#Collect it as tuple and print.
count_list =counts.collect()
for item in count_list:
    print (item[0]+":", item[1])

#Store it
#counts.saveAsPickleFile("/tmp/word_count_english")

sc.stop()