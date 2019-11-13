import sys
import json
from pyspark import SparkContext
from time import time

def task(argv):
    start = time()
    infile, outfile = argv[1], argv[2]

    sc = SparkContext('local[*]','task1')
    adevRDD = sc.textFile(infile)

    adevRDD2 = adevRDD.map(lambda x: json.loads(x))\
        .map(lambda x: (x.get("visitorId"),x.get("id"),x.get("timestamp"),x.get("type"),x.get("pageUrl")))
    validrecords = adevRDD2.groupBy(lambda x: x[0])\
        .filter(lambda x: x[0] and len(x[1])>1).values()
    ouputRDD = validrecords.map(list).map(lambda x: sorted(x,key=lambda i:i[2]))\
        .map(lambda x: [e1+(e2[-1],) for e1,e2 in zip(x,x[1:])]).flatMap(lambda x: x)
    ouputRDD = ouputRDD.map(lambda x:{"id":x[1],"timestamp":x[2],"type":x[3],"visitorId":x[0],"pageUrl":x[4],"nextPageUrl":x[5]})

    with open(outfile,'w') as outf:
        for output in ouputRDD.collect():
            outf.write(json.dumps(output)+"\n")
            
    end = time()
    print("total run time in seconds:",end-start)

if __name__ == '__main__':
    task(sys.argv)
