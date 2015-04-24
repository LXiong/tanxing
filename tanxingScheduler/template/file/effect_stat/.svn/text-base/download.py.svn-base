#coding=utf-8

import urllib2
import sys

if __name__ == "__main__":
    f = urllib2.urlopen(sys.argv[1])  
    with open(sys.argv[2], "w") as code:
        code.write(f.read())
