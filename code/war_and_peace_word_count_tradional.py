#THIS IS A STATE AND WON'T WORK IN HADOOP OR DISTRUBTED SYSTEMS!
import re
import time
start_time = time.time()
word_count = {}
# ^^^^^^^^^^^^^^
with open('../../data/war_and_peace-300.txt', 'r') as read_obj:
    line = 'init'
    while line:
        line = read_obj.readline().lower()
        word_list = re.split('\W+', line)
        for word in word_list:
            if word_count.get(word) == None:
                word_count[word] = 0
            word_count[word] += 1
for key, value in sorted(word_count.iteritems(), key=lambda (k,v): (-v,k)):
    print "%s: %s" % (key, value)

print("--- %s seconds ---" % (time.time() - start_time))