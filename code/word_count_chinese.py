# -*- coding: utf8 -*-
'''

Word count problem for Chinese chars.

By Paul Wu uwzwu@uw.edu

'''

from __future__ import print_function
from util import sc

#Define a function to convert a string to a list.
def to_list(str):
    # print(str)
    ret = []
    for c in str:
        if c.isalpha():
            ret.append(c)
    return ret

if __name__ == '__main__':
    # Read files...can be from hdfs.
    text_file = sc.textFile("../data/ch_words*.txt")
    # Transform the rdd into key-value pair (word, count) and use reduce alg.
    text_file.foreach(print)

    counts = text_file.flatMap(lambda line: to_list(line)) \
        .map(lambda word: (word.lower(), 1)) \
        .reduceByKey(lambda a, b: a + b)
    # print
    # counts.foreach(print)
    counts = counts.sortBy(lambda pair: -pair[1])
    # Collect it as tuple and print.
    count_list = counts.collect()
    for item in count_list:
        print(item[0] + ":", item[1])
    sc.stop()