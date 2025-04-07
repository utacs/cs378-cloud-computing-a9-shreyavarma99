
from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
import sys
from operator import add
import json

def aggregate_count(new_values, total_sum):
    return sum(new_values) + (total_sum or 0)

def decode_json(line):
    dictionary = json.loads(line)
    text = dictionary["text"]
    # extract other fields TODO
    return text

def safe_parse(s):
    try:
        return json.loads(s)
    except json.JSONDecodeError:
        return None

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


# split each tweet into words
#  = dataStream.window(300, 5).flatMap(lambda line: line.split())

#words.pprint(5)


lines = dataStream.window(300, 5).map(decode_json)


lines.pprint(2)


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


