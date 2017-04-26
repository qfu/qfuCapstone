from topMine import frequentMine
from fpGrowth import fpGrowth
from plot import visualize
from os import walk, path
import os
os.environ["SPARK_HOME"] = "/Users/qfu/Desktop/Software/Spark/spark-1.6.2-bin-hadoop2.6"

from pyspark import SparkConf, SparkContext

class Utility:
    @staticmethod
    def calFrequency(item,freq,filename):
        dir = {}
        ss = " ".join(x.encode('utf-8') for x in item)
        dir.setdefault(ss, []).append((int(path.splitext(filename)[0]), freq))
        return dir

    @staticmethod
    def mergeFrequency(dict1, dict2):
        mergeDict = {}
        for (k,v) in dict1.iteritems():
            mergeDict[k] = dict1.get(k)


        for (k,v) in dict2.iteritems():
            mergeDict[k] = mergeDict.setdefault(k,[]) + dict2.get(k)

        return mergeDict


def main():
    # Setting up the standalone mode
    conf = SparkConf().setMaster("local[4]").setAppName("TopMiner")
    sc = SparkContext(conf=conf)


    mypath = "./Data/Date/"

    files = []
    dir = {}

    
    for (dirpath, dirnames, filenames) in walk(mypath):
        files = filenames
        break

    for filename in files:

        key = frequentMine(sc, Filepath= mypath + filename, iteration=7, minimumSupport=3, tweets=True, \
                               perTweet=False, verbose=False).keys()
        print key

        res = fpGrowth(key,0.01,sc)

        res_dir = res.map(lambda (item,freq) : Utility.calFrequency(item,freq,filename)) \
            .reduce(lambda x,y : Utility.mergeFrequency(x,y))
        dir = Utility.mergeFrequency(res_dir,dir)
    print dir

    for key, value in dir.iteritems():
        x,text,y =[],[],[]
        for pair in value:
            x.append(pair[0])
            y.append(pair[1])
            text.append(key)
        print x,y,text
        visualize(x,y,text)



if __name__ == "__main__": main()