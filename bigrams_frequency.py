import sys
 
from pyspark import SparkContext, SparkConf
 
conf = SparkConf()
sc = SparkContext(conf=conf)

# words = sc.textFile(sys.argv[1]).flatMap(lambda line: line.split(" "))
	
# wordCounts = words.map(lambda word: (word, 1)).reduceByKey(lambda a,b:a +b)
	
# wordCounts.coalesce(1, shuffle=True).saveAsTextFile(sys.argv[2])
# sc.stop()
		
words = sc.textFile(sys.argv[1])

bigrams = words.map(lambda line: line.split(" "))
bigrams = bigrams.flatMap(lambda word: [((word[i], word[i+1]),1) for i in range(0, len(word)-1)])

bgCounts = bigrams.reduceByKey(lambda x,y: x+y)

bgCounts.coalesce(1, shuffle=True).saveAsTextFile(sys.argv[2])

sc.stop()