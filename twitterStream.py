from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import operator
import numpy as np
import matplotlib.pyplot as plt
#http://spark.apache.org/docs/latest/streaming-programming-guide.html
#for certain functions and documentation help

def main():
    conf = SparkConf().setMaster("local[2]").setAppName("Streamer")
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, 10)   # Create a streaming context with batch interval of 10 sec
    ssc.checkpoint("checkpoint")

    pwords = load_wordlist("positive.txt")
    #print pwords
    nwords = load_wordlist("negative.txt")

    counts = stream(ssc, pwords, nwords, 100)
    print counts
    make_plot(counts)


def make_plot(counts):
    """
    Plot the counts for the positive and negative words for each timestep.
    Use plt.show() so that the plot will popup.
    """
    
    plt.xlabel("Time step")
    plt.ylabel("Word count")
    
    pos = []
    neg = []


    for i in counts:
        if i:#in case we do not get data in first iteration, ignore null value
            pos.append(i[0][1])
            neg.append(i[1][1])

    plt.plot(range(len(pos)), pos, marker='o', label = 'positive')
    plt.plot(range(len(neg)), neg, marker='o', label = 'negative')

    #help from Ankit Bhandari, team member for this line
    plt.xticks(np.arange(0,len(counts),1))

    plt.legend(loc = 'upper left')
    plt.show()


def load_wordlist(filename):
    """
    This function should return a list or set of words from the given filename.
    """
    
    #list = []
    file = open(filename,"r")
    list = [line.strip() for line in file.readlines()]
    return list


def stream(ssc, pwords, nwords, duration):
    kstream = KafkaUtils.createDirectStream(
        ssc, topics = ['twitterstream'], kafkaParams = {"metadata.broker.list": 'localhost:9092'})
    tweets = kstream.map(lambda x: x[1].encode("ascii","ignore"))

    # Each element of tweets will be the text of a tweet.
    # You need to find the count of all the positive and negative words in these tweets.
    # Keep track of a running total counts and print this at every time step (use the pprint function).
    

    #tweets.pprint()
    words = tweets.flatMap(lambda tweet:tweet.split(" "))
    #words.pprint()

    positive = words.filter(lambda x: (x in pwords))
    negative = words.filter(lambda x: (x in nwords))

    #positive.pprint()
    #negative.pprint()

    ppairs = positive.map(lambda p: ('positive', 1))
    npairs = negative.map(lambda n: ('negative', 1))

    pwordCounts = ppairs.reduceByKey(lambda x, y: x + y)
    nwordCounts = npairs.reduceByKey(lambda x, y: x + y)

    count = pwordCounts.union(nwordCounts)
    #count.pprint()
    #pwordCounts.pprint()
    #nwordCounts.pprint()

    def updateFunction(newValues, runningCount):
        if runningCount is None:
           runningCount = 0
        return sum(newValues, runningCount)

    prunningCounts = pwordCounts.updateStateByKey(updateFunction)
    nrunningCounts = nwordCounts.updateStateByKey(updateFunction)

    #prunningCounts.pprint()
    #nrunningCounts.pprint()

    total = prunningCounts.union(nrunningCounts)
    total.pprint()

    # Let the counts variable hold the word counts for all time steps
    # You will need to use the foreachRDD function.
    # For our implementation, counts looked like:
    #   [[("positive", 100), ("negative", 50)], [("positive", 80), ("negative", 60)], ...]
    counts = []
    count.foreachRDD(lambda t,rdd: counts.append(rdd.collect()))

    ssc.start()                         # Start the computation
    ssc.awaitTerminationOrTimeout(duration)
    ssc.stop(stopGraceFully=True)

    return counts


if __name__=="__main__":
    main()
