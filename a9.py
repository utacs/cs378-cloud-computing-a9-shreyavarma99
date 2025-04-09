from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
import sys
from operator import add
import json

# create spark configuration
conf = SparkConf()
conf.setAppName("TwitterStreamApplication")

# create spark context with the above configuration
sc = SparkContext(conf=conf)

# sc.setLogLevel("INFO")
# sc.setLogLevel("ERROR")
sc.setLogLevel("FATAL")


# create the Streaming Context from the above spark context with interval size 5 seconds
ssc = StreamingContext(sc, 5)


# setting a checkpoint to allow RDD recovery
ssc.checkpoint("checkpoint_TwitterApp")

# read data from port 9009
dataStream = ssc.socketTextStream("localhost", 9009)
windowedDataStream = dataStream.window(300, 5)

# task 1

def task1_decode_json(line):
    dictionary = json.loads(line)
    if "retweeted_status" not in dictionary:
        return None
    if "data" not in dictionary["retweeted_status"]["text"].lower():
        return None
    return dictionary["retweeted_status"]["text"], dictionary["retweeted_status"]["user"]["id"]

task1 = windowedDataStream\
.map(task1_decode_json)\
.filter(lambda x: x is not None)\
.map(lambda x: (x[1], (1, x[0])))\
.reduceByKey(lambda x, y: (x[0] + y[0], x[1]))\
.map(lambda x: (x[0], x[1][0], x[1][1]))\
.transform(lambda x: x.sortBy(lambda x: x[1], ascending=False))\

task1.pprint(5)

# task 2

def task2_decode_json(line):
    dictionary = json.loads(line)
    text = dictionary["text"]
    if "data" not in text.lower():
        return None
    hashtags = [hashtag["text"] for hashtag in dictionary["entities"]["hashtags"]]
    if not hashtags:
        return None
    return dictionary["text"], hashtags

stop_words = set([
    "the", "and", "is", "in", "to", "with", "a", "of", "on", "for", "this", "that", "it", "as", "at"
])
sw = sc.broadcast(stop_words)

def clean_word(word):
    word = word.lower()
    if word in sw.value:
        return None
    if len(word) < 3:
        return None
    if not word.isalpha():
        return None
    return word

task2 = windowedDataStream\
.map(task2_decode_json)\
.filter(lambda x: x is not None)\
.flatMap(lambda x: [
    (hashtag, clean_word(word)) for hashtag in x[1] for word in x[0].split() if clean_word(word) is not None
])\
.map(lambda x: ((x[0], x[1]), 1))\
.reduceByKey(lambda x, y: x + y)\
.transform(lambda x: x.sortBy(lambda x: x[1], ascending=False))\

task2.pprint(10)




# We show an example of top 5 results 

# result_stream = words.map(lambda x: (x.lower(), 1)).reduceByKey(add)\
#     .updateStateByKey(aggregate_count)\
#                      .transform(lambda rdd: rdd.sortBy(lambda x: x[1], ascending=False))
# print 5 elements 
# 
# result_stream.pprint(5)



# start the streaming computation
ssc.start()

# wait for the streaming to finish
ssc.awaitTermination()


