import findspark, os, shutil, csv 
from pyspark import SparkContext, SparkConf
reviewersPath = "/mnt/c/Users/vikto/OneDrive/Dokumenter/NTNU/TDT4305BigData/yelp_top_reviewers_with_reviews.csv"

findspark.init()
sparkConf = SparkConf().setAppName("Yelp").setMaster("local")
sc = SparkContext(conf = sparkConf)

##### Subtask 1 ##### (Subtask)

### Load the dataset
reviewersRDD = sc.textFile(reviewersPath)

### Load sentiment lexicons i.e. list of positive words and list of negative words

### Find the polarity of each review

### Find top 'k' businesses with most positive reviews.



##### Subtask 2 ##### (How to find polarity of reviews?)

### First step is to tokenize each review into individuall words.

### Remove all the stopwords.

### Find all positive and negative expressions



##### Subtask 3 ##### (How to get top 'k' businesses)

### Sort the businesses in descending order based on sentiment score.

### Save the results using saveAsTextFile() method