import os

from pyspark import SparkConf, SparkContext

from pyspark.mllib.fpm import FPGrowth

#os.environ["SPARK_HOME"] = "/Users/qfu/Desktop/Software/Spark/spark-1.6.2-bin-hadoop2.6"


def fpGrowth(input,support,sc):
    #rdd = sc.parallelize(input)
    rdd = sc.parallelize(input).map(lambda key : list(set(key.split(" "))))
    #print rdd.collect()
    model = FPGrowth.train(rdd, minSupport=support, numPartitions=10)
    #frequentItems = sorted(model.freqItemsets().collect())
    #return rdd.collect()
    return model.freqItemsets()