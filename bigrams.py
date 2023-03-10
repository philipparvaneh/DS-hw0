import sys
 
from pyspark import SparkContext, SparkConf
 
conf = SparkConf()
sc = SparkContext(conf=conf)

# words = sc.textFile(sys.argv[1]).flatMap(lambda line: line.split(" "))
	
# wordCounts = words.map(lambda word: (word, 1)).reduceByKey(lambda a,b:a +b)
	
# wordCounts.coalesce(1, shuffle=True).saveAsTextFile(sys.argv[2])
# sc.stop()
		
words = sc.textFile(sys.argv[1])

wordCounts = words.flatMap(lambda line: line.split(" "))
wordCounts = wordCounts.map(lambda word: (word, 1)).reduceByKey(lambda a,b: a+b)

bigrams = words.map(lambda line: line.split(" "))
bigrams = bigrams.flatMap(lambda word: [((word[i], word[i+1]),1) for i in range(0, len(word)-1)])

bgCounts = bigrams.reduceByKey(lambda x,y: x+y)
bgCounts = bgCounts.map(lambda x: (x[0][0], (x[0][1], x[1])))

final = bgCounts.join(wordCounts)
final = final.map(lambda x: ((x[0], x[1][0][0]), x[1][0][1]/x[1][1]))

final.coalesce(1, shuffle=True).saveAsTextFile(sys.argv[2])

sc.stop()