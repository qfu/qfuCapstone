import json
import fnmatch
import os


class Utility:
    @staticmethod
    def files():
        inputPath = "./Data/tweets_smaller.txt"

        with open(inputPath,'r') as infile:
            for line in infile:
                tweet = json.loads(line)
                filename = "./Data/Date/" + str(tweet['firstpost_date']) + '.txt'

                mode = 'a' if os.path.exists(filename) else 'w'
                with open(filename,mode) as outfile:
                    outfile.write(line)
        infile.close()


def main():
    Utility.files()


if __name__ == "__main__": main()