
import argparse
import json
import pyspark
import time

#  python task3_default.py --input_file ./data/review.json --output_file data/a1t3_default.json
if __name__ == '__main__':

    start_time = time.process_time()
    
    sc_conf = pyspark.SparkConf()
    sc_conf.setAppName('task1')
    sc_conf.setMaster('local[*]')
    sc_conf.set('spark.driver.memory', '8g')
    sc_conf.set('spark.executor.memory', '4g')
    sc = pyspark.SparkContext(conf=sc_conf)
    sc.setLogLevel("OFF")

    # reading in parameters
    parser = argparse.ArgumentParser(description='ALT1')
    parser.add_argument('--input_file', type=str,
                        default='./backup/data/hw1/review.json', help='the input file')
    parser.add_argument('--output_file', type=str,
                        default='./backup/data/hw1/a1t3_default.json', help='output file containing answers')
    parser.add_argument('--n', type=int, default=5,
                        help='more than n reviews')

    args = parser.parse_args()
    import operator
    # business_review = sc.textFile(args.input_file).map(lambda line: json.loads(line)).map(
    #     lambda x: (x['business_id'], 1)).reduceByKey(operator.add).filter(lambda x: x[1] > args.n)
    

    business_review = sc.textFile(args.input_file).map(lambda line: json.loads(line)).map(
        lambda x: (x['business_id'], 1)).mapPartitions(lambda x: [sum(1 for _ in x)])
    # temp = .reduceByKey(operator.add).filter(lambda x: x[1] > args.n)

    h = sc.textFile(args.input_file).map(lambda line: json.loads(line)).map(
        lambda x: (x['business_id'], 1))
    
    sol = {}

    sol['n_partitions'] = business_review.getNumPartitions()

    '''
    In PySpark, you can use the glom() method of an RDD to get the number of items per partition. 
    The glom() method collects the items of each partition in the RDD into an array, 
    so you can get the length of each array to get the number of items per partition.
    '''  # from google
    n_items = business_review
    sol['n_items'] = n_items.collect()

    result = h.reduceByKey(operator.add).filter(lambda x: x[1] > args.n).collect()
    sol['results'] = result

    sc.stop()

    f = open(args.output_file, 'w', encoding='utf-8')
    json.dump(sol, f)
    f.close()

    ending_time = time.process_time() 
    elapsed_time = ending_time - start_time # gets the time elapsed in seconds 
    print(f'elapsed time in milliseconds: {elapsed_time * 1000}')
    
    print(sol['n_partitions'])