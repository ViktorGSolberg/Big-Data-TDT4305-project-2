import findspark, os, shutil, base64, string 
from pyspark import SparkContext, SparkConf

DIRECTORY_PATH = '/mnt/c/Users/vikto/OneDrive/Dokumenter/NTNU/TDT4305BigData'
RESULTS_PATH = '/mnt/c/Users/vikto/OneDrive/Dokumenter/NTNU/TDT4305BigData/results_exercise2'
REVIEWS_PATH = DIRECTORY_PATH + '/yelp_top_reviewers_with_reviews.csv'
AFINN_PATH = DIRECTORY_PATH + '/AFINN.txt'
STOPWORDS_PATH = DIRECTORY_PATH + '/stoppwords.txt'

### Initialising pyspark
findspark.init()
sparkConf = SparkConf().setAppName("Yelp").setMaster("local")
sc = SparkContext(conf = sparkConf)

### Loading all of the datasets and textfiles into RDDs
def load_RDDs(reviews_path, afinn_path, stopwords_path):
    return sc.textFile(reviews_path), sc.textFile(afinn_path), sc.textFile(stopwords_path)

### Transforming all of the stopwords from RDD to list
### Format: [stopword1, stopword2, ...]
def get_list_of_stopwords(stopwordsRDD):
    return stopwordsRDD.map(lambda line: line.split('\t')[0].encode('ascii', 'ignore')).collect()

### Transforming all of the afinnwords to a dictionary
### Format: {word1: value1, word2: value2, ...}
def get_afinn_dictionary(afinnRDD):
    return afinnRDD.map(lambda line: (line.split('\t')[0].encode('ascii', 'ignore'),
                                      line.split('\t')[1].encode('ascii', 'ignore')
                                      )).collectAsMap()

### Reducing the reviewsRDD to business_id and corresponding review
### Format: [(header1, header2), (business1, review1), (business2, review2), ...]
def get_businessids_and_reviews(reviewsRDD):
    return reviewsRDD.map(lambda row: (row.split()[2], row.split()[3]))

### Removing the headers from the reviewsRDD
### Format: [(business1, review1), (business2, review2), ...]
def remove_headers(reviewsRDD):
    headers = reviewsRDD.first()
    return reviewsRDD.filter(lambda row: row != headers)

### Preprocessing the reviews (decoding, removing whitespaces and punctuations,
###                            lowering words, tokenizing)
### Format: [(business1, [review1_word1, review1_word2, ...]), 
###          (business2, [review1_word1, review1_word2, ...]), ...]
def preprocess_reviews(reviewsRDD):
    return reviewsRDD.map(lambda row: (row[0].encode('ascii', 'ignore'),
           base64.b64decode(row[1]).translate(None, string.punctuation).lower().split() 
           ))

### Help function for removing stopwords from reviews
### Format: [word1, word2, ...]
def remove(review, stopwords):
    return [word for word in review if word not in stopwords and word != '']

### Removing all of the stopwords from the reviews
### Format: [(business1, [review1_word1, review1_word2, ...]), 
###          (business2, [review1_word1, review1_word2, ...]), ...]
def remove_stopwords(reviewsRDD, stopwords):
    return reviewsRDD.map(lambda tupple: (tupple[0], remove(tupple[1], stopwords)))

### Transforms a list of words to corresponding afinn values
### Format: [value1, value2, ...]
def map_words_to_values(review_words, afinn_dict):
    return [afinn_dict[word] for word in review_words if word in afinn_dict]

### Transforms the reviews to a list of corresponding afinn values
### Format: [(business1, [value1, value2, ...]), 
###          (business2, [value1, value2, ...]), ...] 
def transform_reviews_to_values(reviewsRDD, afinn_dict):
    return reviewsRDD.map(lambda tupple: (tupple[0], map_words_to_values(tupple[1], afinn_dict)))

### Help function for summarizing afinn values for a review
### Format: value
def summarize_values(values):
    return sum(int(word) for word in values)

### Summarizing affin values for all reviews
### Format: [(business1, value1), (business2, value2), ...]
def summarize_all_values(reviewsRDD):
    return reviewsRDD.map(lambda tupple: (tupple[0], summarize_values(tupple[1])))

### Summarizing the afinn values for everey business
### Format: [(business1, total_score1), (business2, total_score2), ...]
def get_business_scores(reviewsRDD):
    return reviewsRDD.reduceByKey(lambda a, b: a+b)

### Ordering the businesess by total value of afinn values, descending order
### Format [(business1, highest_score), (business2, second_highest_score), ...]
def get_top_k_businesses(reviewsRDD, k):
    return reviewsRDD.takeOrdered(k, key = lambda x: -x[1])


def main(print_result, k):
    reviewsRDD_with_headers, afinnRDD, stopwordsRDD = load_RDDs(REVIEWS_PATH, AFINN_PATH, STOPWORDS_PATH)
    stopwords = get_list_of_stopwords(stopwordsRDD)
    afinn_dictionary = get_afinn_dictionary(afinnRDD)

    reviewsRDD_ids_and_reviews = get_businessids_and_reviews(reviewsRDD_with_headers)
    reviewsRDD_removed_headers = remove_headers(reviewsRDD_ids_and_reviews)
    reviewsRDD_tokenized = preprocess_reviews(reviewsRDD_removed_headers)
    reviewsRDD_removed_stopwords = remove_stopwords(reviewsRDD_tokenized, stopwords)
    reviewsRDD_afinn_values = transform_reviews_to_values(reviewsRDD_removed_stopwords, afinn_dictionary)
    reviewsRDD_summarized_values = summarize_all_values(reviewsRDD_afinn_values)
    reviewsRDD_business_scores = get_business_scores(reviewsRDD_summarized_values)
    most_popular_businesses = get_top_k_businesses(reviewsRDD_business_scores, k)

    if print_result:
        for tupple in most_popular_businesses:
            print('Business: ' + tupple[0] + " Score: " + str(tupple[1]))
    
    if os.path.isdir(RESULTS_PATH):
        shutil.rmtree(RESULTS_PATH)
    results = sc.parallelize(most_popular_businesses).repartition(1)
    results.saveAsTextFile(RESULTS_PATH)

if __name__ == "__main__":
    main(False, 20)