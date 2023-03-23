import argparse
import json
import time
import pyspark



def main(train_file, test_file, model_file, output_file, n_weights, sc):

    rdd = sc.textFile(train_file).map(lambda x: json.loads(x))

    # rdd of values we want to predict the ratings for 
    predict_rdd = sc.textFile(test_file).map(lambda x: json.loads(x)) \
        .map(lambda x: (x['user_id'], x['business_id'])) # pairs to predict 
    
    # what items the user has rated 
    user_rated_items = rdd.map(lambda x: (x['user_id'], x['business_id'])) \
        .groupByKey() \
        .map(lambda x: (x[0], list(x[1])))
    # note model_file will contain the weights 
    weights = sc.textFile(model_file).map(lambda x: json.loads(x)) \
        .map(lambda x: ((x["b1"], x["b2"]), x['sim'])) \
        .map(lambda x: (frozenset(x[0]), x[1])) 
    weights = sc.broadcast(weights.collectAsMap())
    # TODO: Write a map step that allows you to filter out which pairs exist inside the weights  
    existing_user_ratings = rdd.map(lambda x: ((x['user_id'], x['business_id']), x['stars']))
        
 
    # (('user_id', 'b_id'), rating)
    existing_user_ratings = sc.broadcast(existing_user_ratings.collectAsMap())

    # user_id, (b_id to predict, [list of b_id that user_id has rated]) 
    # user_id, (b_id to predict, b_id that user has rated) the tuple is sorted to ensure that it exists 
    
    # gives us (user, (predict b_id, b_id), sim)
    highest_user_rated = predict_rdd.join(user_rated_items).flatMap(lambda x: [(x[0], (x[1][0], i)) for i in x[1][1]]).map(lambda x: (x[0], frozenset(x[1]))).filter(lambda x: x[1] in weights.value).map(lambda x: (x[0], x[1], weights.value[x[1]]))
    def lookup_sim(x):

        return None 

    #TODO just map out the value we dont need (the business we want to predict)
    def makePrediction(x): # TODO: figure out a way to get the rdd above to group by key 
        user_id = x[0] 
        businesses = x[1]
        item_to_predict = business_pair_rdd[0][0]
        # numerator 
        ratings_and_weights = 0 
        # denominator 
        weight_sums = 0 
        for i in range(len(businesses)):
            ratings_and_weights += user_business_stars.value[frozenset((user_id, businesses[0][1]))]
            weight_sums += businesses[1]
        predicted_score = 0 
        if weight_sums != 0:              
            predicted_score = ratings_and_weights/weight_sums

        return ((user_id, item_to_predict), predicted_score)
    print()

if __name__ == '__main__':
    start_time = time.time()
    sc_conf = pyspark.SparkConf() \
        .setAppName('hw3_task2') \
        .setMaster('local[*]') \
        .set('spark.driver.memory', '4g') \
        .set('spark.executor.memory', '4g')
    sc = pyspark.SparkContext(conf=sc_conf)
    sc.setLogLevel("OFF")

    parser = argparse.ArgumentParser(description='hw3')
    parser.add_argument('--train_file', type=str, default='./data/train_review.json')
    # parser.add_argument('--test_file', type=str, default='./data/test_review.json')
    parser.add_argument('--test_file', type=str, default='./data2/val_review.json')
    parser.add_argument('--model_file', type=str, default='./outputs/task2.case1.model')
    parser.add_argument('--output_file', type=str, default='./outputs/task2.case1.test.out')
    parser.add_argument('--time_file', type=str, default='./outputs/time.out')
    parser.add_argument('--n', type=int, default=3)
    args = parser.parse_args()

    main(args.train_file, args.test_file, args.model_file, args.output_file, args.n, sc)
    sc.stop()

    # log time
    with open(args.time_file, 'w') as outfile:
        json.dump({'time': time.time() - start_time}, outfile)
    print('The run time is: ', (time.time() - start_time))
