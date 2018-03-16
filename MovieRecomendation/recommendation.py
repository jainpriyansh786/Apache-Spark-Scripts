from pyspark import SparkContext , SparkConf
from pyspark.mllib.recommendation import ALS
import os
import math


#os.environ["PYSPARK_PYTHON"] = "/usr/local/bin/python3"
conf = SparkConf().setAppName("MovieRecommendation")

sc = SparkContext(conf = conf)
rating_file = sc.textFile("./data/ratings.csv")
movie_file = sc.textFile("./data/movies.csv")

rating_header = rating_file.first()
rating_final =     rating_file.filter(lambda line : line != rating_header)
rating = rating_final.map(lambda line : line.split(",")).map(lambda line : (int(line[0]),int(line[1]),float(line[2]))).cache()


movie_header = movie_file.first()
movie_final =     movie_file.filter(lambda line : line != movie_header)
movie = movie_final.map(lambda line : line.split(",")).map(lambda line : (int(line[0]),(line[1]))).cache()


training_rating , test_rating = rating.randomSplit([7,3],seed=0)

seed = 5
iterations  = 10
regular_parameter = 0.1
rank = 8

#model = ALS.train(training_rating, rank , seed=seed, iterations=iterations, lambda_=regular_parameter)
#test_rating_wolabel = test_rating.map(lambda x : (x[0],x[1]))

#predicted  = model.predictAll(test_rating_wolabel).map(lambda r: ((r[0], r[1]), r[2]))

#actual_and_predicted = test_rating.map(lambda x : ( (int(x[0]) , int(x[0])) , (float(x[2])))).join(predicted)

#training_error = math.sqrt(actual_and_predicted.map(lambda r: (r[1][0] - r[1][1])**2).mean())


def calculateavg(movie_id_with_ratings):
    num_ratings = len(movie_id_with_ratings[1])

    avg = sum(movie_id_with_ratings[1]) / num_ratings

    return (movie_id_with_ratings[0], num_ratings, avg)

movie_with_rating_list = rating.map(lambda x : (x[1],x[2])).groupByKey()
movie_with_avg_rating = movie_with_rating_list.map(calculateavg)
movie_with_id_avg_rating = movie_with_avg_rating.map(lambda x : (x[0],x[1]))
user_id = 0
user_rating = sc.parallelize([(0, 260, 9), (0, 1, 8), (0, 16, 7), (0, 25, 8), (0, 32, 9), (0, 335, 4), (0, 379, 3), (0, 296, 7), (0, 858, 10), (0, 50, 8)])

new_rating = rating.union(user_rating)

model = ALS.train(new_rating, rank , seed=seed, iterations=iterations, lambda_=regular_parameter)

user_movie_id = list(user_rating.map(lambda x : x[1]).collect())
rating_wo_user_movie_id = rating.filter(lambda x: x[0] not in user_movie_id)
user_id_movie_id  = rating_wo_user_movie_id.map(lambda line  : (user_id,line[1]))
predicted = model.predictAll(user_id_movie_id)
new_user_recommendations_rating = predicted.map(lambda x: (x.product, x.rating))
new_user_recommendations_rating_title_and_count = new_user_recommendations_rating.join(movie).join(movie_with_id_avg_rating).distinct()
title_rating_count = new_user_recommendations_rating_title_and_count.map(lambda x : (x[1][0][1] , x[1][0][0],x[1][1]))
top_25_movies = title_rating_count.filter(lambda x : x[2] > 25).takeOrdered(25 , key = lambda x : -x[1])

print(top_25_movies)






