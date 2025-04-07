
from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
import sys
from operator import add

def aggregate_count(new_values, total_sum):
    return sum(new_values) + (total_sum or 0)


# create spark configuration
conf = SparkConf()
conf.setAppName("TwitterStreamApplication")

# create spark context with the above configuration
sc = SparkContext(conf=conf)

# sc.setLogLevel("INFO")
# sc.setLogLevel("ERROR")
sc.setLogLevel("FATAL")


# create the Streaming Context from the above spark context with interval size 2 seconds
ssc = StreamingContext(sc, 2)


# setting a checkpoint to allow RDD recovery
ssc.checkpoint("checkpoint_TwitterApp")

# read data from port 9009
dataStream = ssc.socketTextStream("localhost", 9009)


# split each tweet into words
# A window of 30 seconds and sliding 1 second 
words = dataStream.window(300, 10).flatMap(lambda line: line.split())


# Add your code here ...

# We show an example of top 5 results 

result_stream = words.map(lambda x: (x.lower(), 1)).reduceByKey(add)\
    .updateStateByKey(aggregate_count)\
                     .transform(lambda rdd: rdd.sortBy(lambda x: x[1], ascending=False))




# print 5 elements 
# 
result_stream.pprint(5)

# start the streaming computation
ssc.start()

# wait for the streaming to finish
ssc.awaitTermination()


