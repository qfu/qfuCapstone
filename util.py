import json
import fnmatch
import os
import time
from datetime import datetime

from pyspark import SparkConf, SparkContext
os.environ["SPARK_HOME"] = "/Users/qfu/Desktop/Software/Spark/spark-1.6.2-bin-hadoop2.6"

class Utility:
    @staticmethod
    def writeNewFile(line):
        print line
        tweet = json.loads(line)

        if tweet['tweet'] == None:
            return

        if(tweet['tweet'].get('created_at') != None):
            date_str = tweet['tweet']['created_at']
        else:
            return

        time_struct = time.strptime(date_str, "%a %b %d %H:%M:%S +0000 %Y")#Tue Apr 26 08:57:55 +0000 2011
        #print time_struct, "time_struct"
        date = datetime.fromtimestamp(time.mktime(time_struct))
        date_s = str(date.year)+ str(date.month) + str(date.day)

        filename = "./Data/Date/" + date_s + '.txt'

        mode = 'a' if os.path.exists(filename) else 'w'
        with open(filename,mode) as outfile:
            outfile.write(line + '\n')
        outfile.close()
    @staticmethod
    def set_date(line):
        """Convert string to datetime
        """
        tweet = json.loads(line)
        date_str = tweet['tweet']['created_at']
        time_struct = time.strptime(date_str, "%a %b %d %H:%M:%S +0000 %Y")#Tue Apr 26 08:57:55 +0000 2011
        #print time_struct, "time_struct"
        date = datetime.fromtimestamp(time.mktime(time_struct))
        date_s = str(date.year)+ str(date.month) + str(date.day)
        return date_s

def files():
    inputPath = "./Data/tweets_smaller.txt"
    conf = SparkConf().setMaster("local[4]").setAppName("Utility")
    sc = SparkContext(conf=conf)
    
    lines = sc.textFile(inputPath)

    lines.map( lambda line : Utility.writeNewFile(line)).count()



def main():
    files()


if __name__ == "__main__": main()