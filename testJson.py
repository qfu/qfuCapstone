import json
import os

def t():
    inputPath = "./Data/Date/201511.txt"
    with open(inputPath) as file:
        start = 1
        for line in file:
            js = json.loads(line)
            print js
            print start
            start += 1

def main():
    t()


if __name__ == "__main__": main()