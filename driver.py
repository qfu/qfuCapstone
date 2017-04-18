from topMine import frequentMine
from fpGrowth import fpGrowth
from plot import visualize
from os import walk, path
import os
os.environ["SPARK_HOME"] = "/Users/qfu/Desktop/Software/Spark/spark-1.6.2-bin-hadoop2.6"

from pyspark import SparkConf, SparkContext

def main():
    # Setting up the standalone mode
    conf = SparkConf().setMaster("local[4]").setAppName("TopMiner")
    sc = SparkContext(conf=conf)


    mypath = "./Data/Date/"
    dir = {}
    x = []
    text = []
    y = []
    for (dirpath, dirnames, filenames) in walk(mypath):
        for filename in filenames:
            key = frequentMine(sc, Filepath=mypath + filename, iteration=7, minimumSupport=2, tweets=True, \
                               perTweet=False, verbose=False).keys()

            res = fpGrowth(key,0.01,sc)

            for item,freq in res:
                #print item
                ss = " ".join(x.encode('utf-8') for x in item)
                dir.setdefault(ss,[]).append((int(path.splitext(filename)[0]),freq))
                """
                text.append(ss)
                x.append(freq)
                y.append(int(path.splitext(filename)[0]))
                """
        break
    for key, value in dir.iteritems():
        print key,value

    """"
    dir = ""
    fileList = []

    This part is treated each day as one transaction
    for (dirpath, dirnames, filenames) in os.walk(transaction_path):
        fileList = filenames
        dir = dirpath
        break

    fpInput = []
    for file in fileList:
        file_path = dir + file
        #TopMine
        key = frequentMine(sc,Filepath=file_path, iteration=5, minimumSupport = 3, tweets=True, \
                     perTweet=False, verbose=False).keys()
        print key

        fpInput.append(key)
    res = fpGrowth(fpInput,0.6,sc)
    print res
    """




if __name__ == "__main__": main()