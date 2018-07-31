'''

By Paul Wu uwzwu@uw.edu

'''
from __future__ import print_function
from util import spark, sc

input = sc.textFile("../data/numbers.txt")

numbers = input.flatMap(lambda line: line.split('\t'))
numbers.foreach(print)

sums = numbers.reduce(lambda a, b: float(a) + float(b))
print ('sum={0}'.format(sums))
print ('avg={0}'.format(sums/numbers.count()))
