from pyspark import SparkContext
from collections import defaultdict
from collections import OrderedDict
import sys
import os
from itertools import chain, combinations
from operator import add
from itertools import izip
from pyspark.sql import SparkSession
import itertools
import time
import math
from collections import OrderedDict
from collections import Counter
from random import randint
import networkx as nx
from networkx import edge_betweenness_centrality
import itertools
from pyspark.streaming import StreamingContext



# Setup
sc = SparkContext('local[*]','dgim')
sc.setLogLevel(logLevel="OFF" )
ssc = StreamingContext(sc, 10)

lines= ssc.socketTextStream("localhost",9999)

def save(c):
	global thousand
	global dgim 
	global counters

	
	calc = 0
	a = c.collect()
	a = [int(i) for i in a]
	thousand.extend(a)
	thousand = thousand[-1000:]
	actual = sum(thousand)

	

    

	for j in range(0,len(a)):
		if counters >= 1000:
			dele = counters - 1000
			for i in dgim:
				if dele in dgim[i]:
					dgim[i].remove(dele)


		counters = counters + 1
		if a[j]==1:
			if 0 not in dgim:
				dgim[0] = [counters]	
			else:
				dgim[0].extend([counters])
				for i in range(0,len(dgim)):
					if len(dgim[i]) >= 3:
						b = dgim[i]
						dgim[i] = [b[-1]]
						if i+1 not in dgim:
							dgim[i+1] = [b[-2]]
					  	else:
					  		dgim[i+1].extend([b[-2]])
	
	#print(dgim)

	if counters >= 1000:
		for i in range(len(dgim)):
			#print("raised to",i)
			calc = calc + ( (2**i)* (len(dgim[i])))

		ind = len(dgim)-1
		tempo = (2**ind * len(dgim[ind])) / 2
		calc = calc - tempo
		print("Estimated number of ones in the last 1000 bits {}".format(calc))


	if len(thousand) == 1000:
		print ("Actual number of ones in the last 1000 bits: {}".format(actual))
		print("\n")



	

thousand = []
dgim = {}
counters = 0
window = lines.foreachRDD(save)


ssc.start()             # Start the computation
ssc.awaitTermination()  # Wait for the computation to terminate
