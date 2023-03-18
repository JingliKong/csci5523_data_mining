import argparse
import json
import time
import pyspark
import itertools
import operator 
import math 

def main(train_file, model_file, co_rated_thr, sc):

    """ you need to write your own code """
    rdd = sc.textFile(train_file).map(lambda x: json.loads(x))

    user_rdd = rdd.map(lambda x: x['user_id']).distinct()
    # broadcasting the lookup table to know which index of the vector corresponds to each user 
    user_translation = user_rdd.zipWithIndex().map(lambda x: (x[0], x[1])).collectAsMap()
    user_translation = sc.broadcast(user_translation)

    # (business_id, user_id)
    pairs = rdd.map(lambda x: (x['user_id'], x['business_id'], x['stars'])) \
        .map(lambda x: ((x[0], x[1]), x[2])) \
    # dict like: (user, b_id): stars 
    # ('g9RJ3rbeIp5ZbMnGPqvOUA', 'dChRGpit9fM_kZK5pafNyA'): 5.0
    # Then we know how many stars each user rated each business. 
    
    user_business_stars = sc.broadcast(pairs.collectAsMap())

    # (user, [(b1, b2), (b2, b4)]) 
    user_b_rdd = rdd.map(lambda x: (x['user_id'], x['business_id'])) \
        .groupByKey() \
        .map(lambda x: (x[0], sorted(list(itertools.combinations(sorted(x[1]), 2)))))
    # (user1, [(b1, b2), (b3, b4)]) (user2, [(b1, b2), (b1, b3)])
    # [(user1, (b1,b2), (user2, (b1, b3))]
    # (b1, b2) [user1, user2]
    business_pair_rdd = user_b_rdd.flatMap(lambda x: [(x[0], business_pair) for business_pair in x[1]]) \
        .map(lambda x: (x[1], x[0])) \
        .groupByKey() \
        .filter(lambda x: len(x[1]) >= co_rated_thr) \
        .map(lambda x: (x[0], list(x[1])))
    # finding the mean rating for every business 
    
    # business_id, #reviews 
    # [('eFvzHawVJofxSnD7TgbZtg', 169)] 
    num_reviews = rdd.map(lambda x: (x['business_id'], x['stars'])) \
        .groupByKey() \
        .map(lambda x: (x[0], len(x[1])))
    total_stars = rdd.map(lambda x: (x['business_id'], x['stars'])).reduceByKey(operator.add)

    avg_business_stars = total_stars.join(num_reviews).map(lambda x: (x[0], (x[1][0]/x[1][1])))
    avg_business_stars = sc.broadcast(avg_business_stars.collectAsMap())

    def PearsonCorrelation(x):
        # x: (b1, b2) [user1, user2]
        # users who rated this business pair 
        users = x[1]
        business_pair = x[0]
        item_i = business_pair[0]
        item_j = business_pair[1]
        numerator = 0 
        first_sqrt = 0 
        second_sqrt = 0 
        for u in users: 
            temp1 = user_business_stars.value[(u, item_i)] - avg_business_stars.value[item_i]
            temp2 = user_business_stars.value[(u, item_j)]-avg_business_stars.value[item_i]
            numerator +=  temp1 * temp2
            first_sqrt += temp1**2 
            second_sqrt += temp2**2
        first_sqrt = math.sqrt(first_sqrt)
        second_sqrt = math.sqrt(second_sqrt) 

        if first_sqrt == 0 or second_sqrt == 0: 
            result = 0 
        else:
            result = numerator / (first_sqrt * second_sqrt)
        # ((b1, b2), weight) 
        return (business_pair, result) 
    
    weights = business_pair_rdd.map(lambda x: PearsonCorrelation(x))

    json_rdd = weights.map(lambda x: {"b1": x[0][0], "b2": x[0][1], "sim": x[1]}).collect() 
    # Save the RDD as a JSON file
    # print(len(json_rdd))
    f = open(model_file, 'w', encoding='utf-8')
    for pair in json_rdd: 
        json.dump(pair, f)
        f.write('\n')
    f.close()
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
    # parser.add_argument('--train_file', type=str, default='./data/train_review.json')
    parser.add_argument('--train_file', type=str, default='./data2/train_review.json')
    parser.add_argument('--model_file', type=str, default='./outputs/task2.case1.model')
    parser.add_argument('--time_file', type=str, default='./outputs/time.out')
    parser.add_argument('--m', type=int, default=3)
    args = parser.parse_args()

    main(args.train_file, args.model_file, args.m, sc)
    sc.stop()

    # log time
    with open(args.time_file, 'w') as outfile:
        json.dump({'time': time.time() - start_time}, outfile)
    print('The run time is: ', (time.time() - start_time))