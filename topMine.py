import json
import re
import os
from nltk.corpus import stopwords

from pyspark import SparkConf, SparkContext
from nltk.tokenize import RegexpTokenizer

# os.environ["SPARK_HOME"] = "/Users/qfu/Desktop/Software/Spark/spark-1.6.2-bin-hadoop2.6"

# Setting up the standalone mode
# conf = SparkConf().setMaster("local[4]").setAppName("TopMiner")
# sc = SparkContext(conf=conf)

tokenizer = RegexpTokenizer(r'\w+')
#tokenizer = RegexpTokenizer('[A-Z]\w+')
class TweetPreProcess:
    def __init__(self):
        self.emoticons_str = r"""
            (?:
                [:=;] # Eyes
                [oO\-]? # Nose (optional)
                [D\)\]\(\]/\\OpP] # Mouth
            )"""

        self.regex_str = [
            self.emoticons_str,
            r'<[^>]+>', # HTML tags
            r'(?:@[\w_]+)', # @-mentions
            r"(?:\#+[\w_]+[\w\'_\-]*[\w_]+)", # hash-tags
            r'http[s]?://(?:[a-z]|[0-9]|[$-_@.&amp;+]|[!*\(\),]|(?:%[0-9a-f][0-9a-f]))+', # URLs

            r'(?:(?:\d+,?)+(?:\.?\d+)?)', # numbers
            r"(?:[a-z][a-z'\-_]+[a-z])", # words with - and '
            r'(?:[\w_]+)', # other words
            r'(?:\S)' # anything else
        ]

        self.exclude_str = [
            self.emoticons_str,
            r'<[^>]+>', # HTML tags
            r'(?:@[\w_]+)', # @-mentions
            r"(?:\#+[\w_]+[\w\'_\-]*[\w_]+)", # hash-tags
            r'http[s]?://(?:[a-z]|[0-9]|[$-_@.&amp;+]|[!*\(\),]|(?:%[0-9a-f][0-9a-f]))+', # URLs

        ]
        self.exclude_re = re.compile(r'(' + '|'.join(self.exclude_str) + ')', re.VERBOSE | re.IGNORECASE)
        self.tokens_re = re.compile(r'('+'|'.join(self.regex_str)+')', re.VERBOSE | re.IGNORECASE)
        self.emoticon_re = re.compile(r'^'+self.emoticons_str+'$', re.VERBOSE | re.IGNORECASE)


    def tokenize(self,s):
        return self.exclude_re.findall(s)

    def preprocess(self,s, lowercase=False):
        exclude_tokens = self.tokenize(s)
        if lowercase:
            tokens = [token for token in tokenizer.tokenize(s) if token not in exclude_tokens and token not in stopwords.words('english')]
        return tokens



class Utility:
    @staticmethod
    def ngrams(input, index, n,phasefrequency,frequecy,minFrequency):
        #Parse the input
        input = input.split(' ')
        output = []

        #Special case
        if(n == 1):
            for i in range(len(input)):
                output.append(i) # append index
            return {index:output}

        for subindex in phasefrequency.get(index):
            # range(x,y) -> x,y-1
            # Make sure it doesn't exceed the range
            if(subindex + n - 2 >= len(input)):
                continue;

            string = " ".join([ input[i] for i in range(subindex, subindex + n - 1)])
            if frequecy.has_key(string) and frequecy.get(string) >= minFrequency:
                output.append(subindex) #append index
        return {index:output}

    @staticmethod
    def mergeDict(dict1, dict2):
        return dict(dict1.items()+ dict2.items())


    @staticmethod
    def countPhase(input, index, findDict,n):
        input = input.split(' ')
        dict = {}
        if(len(input) == 0):
            return dict

        # document -> indices
        list = findDict[index]

        #Expand n + 1 grams
        for subindex in list:
            if(subindex + n - 1 >= len(input)): continue;
            # x to x+n-1
            string = " ".join([input[i] for i in range(subindex,subindex + n)])
            dict[string] = dict.setdefault(string,0) + 1

        return dict

    @staticmethod
    def mergeFrequency(dict1, dict2):
        mergeDict = {}
        for (k,v) in dict1.iteritems():
            mergeDict[k] = dict1.get(k)


        for (k,v) in dict2.iteritems():
            mergeDict[k] = mergeDict.setdefault(k,0) + dict2.get(k);

        return mergeDict

    @staticmethod
    def loadJson(line):
        formatStr = ""

        tweet = json.loads(line)
        if(tweet == None):
            return formatStr + "\n"
        tpp = TweetPreProcess()
        if (tweet.get('text') != None and tweet.get('lang') != None and tweet.get('lang') == 'en'):
            formatStr = " ".join(tpp.preprocess(tweet.get('text'),True)) + "\n";
            #formatStr = " ".join(tokenizer.tokenize(tweet.get('text'))) + "."
        #print formatStr
        return formatStr
    @staticmethod

    def getPerTweet(input, index, gram,findDict):
        original = input
        input = input.split(' ')
        output = {}
        output.setdefault(original, [])
        for idx in findDict[index]:
            output[original].append(" ".join([input[i] for i in range(idx,idx + gram)]))
        return output

    @staticmethod
    def mergeDocument(dict1, dict2):
        mergeDict = {}
        for (k,v) in dict1.iteritems():
            mergeDict[k] = dict1.get(k)


        for (k,v) in dict2.iteritems():
            mergeDict.setdefault(k,[]).append(dict2.get(k))

        return mergeDict



# Driver Program
def frequentMine(sc,Filepath, iteration = 10, minimumSupport = 6, tweets = False, perTweet = False, verbose = False):

    minFrequency = minimumSupport


    # Load in RDD and RDD persist for multiple operation
    lines = sc.textFile(Filepath)

    if tweets:
        lines = lines.flatMap(lambda x : x.split('\n')) \
            .map(lambda line : Utility.loadJson(line))

    #Serires of operations # Get rid of len 0 line 	#get zipWithIndex
    idxSentence = lines.flatMap(lambda line : line.split("\n")) \
                        .filter(lambda line : len(line) > 0 )\
                        .zipWithIndex().persist()

    #Set up datastructure
    phasefrequency = {}
    frequecy = {}
    result = {}
    document = {}

    # Main Logic of Phase Mining
    # i is the i-grams, default to 8
    for i in range(1,iteration):
        #First Map reduce job get the all N-gram that satisfy minimum support
        #And count if i - 1 is frequent
        nGram = idxSentence \
            .map( lambda (x,y): Utility.ngrams(x,y,i,phasefrequency,frequecy,minFrequency))
        if verbose:
            print "Debugging ....."
            print nGram.collect()

        #Merge all possible #document -> index of ngram
        findDict = nGram \
            .reduce( lambda x,y : Utility.mergeDict(x,y))

        #Extra frequent terms
        #For i >= 2, each findDict calculate frequent i - 1 terms index
        if perTweet and i >= 2:
            doc = idxSentence \
                .map(lambda (x,y) : Utility.getPerTweet(x,y,i-1,findDict))

            if not doc.isEmpty():
                doc_val = doc \
                    .reduce( lambda x,y : Utility.mergeDict(x,y))
            document = Utility.mergeDocument(document, doc_val)
            print "document value is ", document

        #Pruning
        idxSentence = idxSentence \
            .filter( lambda (x,y): findDict.has_key(y) \
                and findDict[y] != None \
                and len(findDict[y]) > 0 )

        if verbose:
            print "The gram", i
            print "idxSentence",idxSentence.collect()
            print "The findDict",findDict

        #Second Map reduce job to Count frequency
        frequencyDict = idxSentence \
            .map(lambda (x,y) : Utility.countPhase(x,y,findDict,i))
        #Check Empty
        if frequencyDict.isEmpty():
            break

        MergeFrequency = frequencyDict \
            .reduce(lambda x,y : Utility.mergeFrequency(x,y))

        filterMergeFrequency = MergeFrequency

        #Parallize the result out
        ls = sc.parallelize(filterMergeFrequency.items()) \
            .filter(lambda (x,y) : y >= minFrequency and len(x) > 0) \
                .collect()
        result.update(ls)

        if verbose:
            print "The merged frequency", MergeFrequency
            print "\n\n"
        #Another iteration
        frequecy = MergeFrequency
        phasefrequency = findDict

    #return Last process
    if perTweet:
        return document
    else:
        return result
