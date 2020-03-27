import findspark, os, shutil, csv 
findspark.init()

from pyspark import SparkContext, SparkConf

reviewersPath = "/mnt/c/Users/vikto/OneDrive/Dokumenter/NTNU/TDT4305BigData/yelp_top_reviewers_with_reviews.csv"


sparkConf = SparkConf().setAppName("Yelp").setMaster("local")
sc = SparkContext(conf = sparkConf)

reviewersFile = sc.textFile(reviewersPath)

numberOfLines = reviewersFile.map(lambda line: line.split("\t")).count()
print(numberOfLines)
print("done [x]")