import argparse
import json
import pyspark

import time 

if __name__ == "__main__":
    '''
    def partitioner(x):
    return hash(x)
    '''
    

    sc_conf = pyspark.SparkConf()
    sc_conf.setAppName('task1')
    sc_conf.setMaster('local[*]')
    sc_conf.set('spark.driver.memory', '8g')
    sc_conf.set('spark.executor.memory', '4g')
    sc = pyspark.SparkContext(conf=sc_conf)
    # sc = pyspark.SparkContext.getOrCreate()
    sc.setLogLevel("OFF")

    # reading in parameters
    # parser = argparse.ArgumentParser(description='ALT1')
    # parser.add_argument('--input_file', type=str,
    #                     default='./backup/data/hw1/review.json', help='the input file')
    # parser.add_argument('--output_file', type=str,
    #                     default='./backup/data/hw1/a1t3_customized.json', help='output file containing answers')
    # parser.add_argument('--n_partitions', type=int,
    #                     default=7, help='how many partitions we split the rdd into')
    # parser.add_argument('--n', type=int, default=100,
    #                     help='more than n reviews')

    # args = parser.parse_args()

    parser = argparse.ArgumentParser(description='ALT1')
    parser.add_argument('--input_file', type=str,
                        default='./data/review.json', help='the input file')
    parser.add_argument('--output_file', type=str,
                        default='./data/a1t3_customized.json', help='output file containing answers')
    parser.add_argument('--n_partitions', type=int,
                        default=1, help='how many partitions we split the rdd into')
    parser.add_argument('--n', type=int, default=100,
                        help='more than n reviews')

    args = parser.parse_args()

    
    start_time = time.process_time()
    business_review = sc.textFile(args.input_file).map(lambda line: json.loads(line)).map(
        lambda x: (x['business_id'], 1)).reduceByKey(lambda x, y: x+y).filter(lambda x: x[1] > args.n).partitionBy(args.n_partitions, hash)

    sol = {}

    n_partitions = business_review.getNumPartitions()
    sol['n_partitions'] = n_partitions

    n_items = business_review.glom().map(len).collect()
    sol['n_items'] = n_items

    result = business_review.collect()
    sol['result'] = result

    sc.stop()

    f = open(args.output_file, 'w', encoding='utf-8')
    json.dump(sol, f)
    f.close()

    ending_time = time.process_time() 
    elapsed_time = ending_time - start_time # gets the time elapsed in seconds 
    print(f'elapsed time in milliseconds: {elapsed_time * 1000}')

    print(sol['n_partitions'])
