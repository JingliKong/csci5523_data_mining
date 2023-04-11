import pyspark
import argparse
import time
import graph_utils

def main(input_file, betweeness_output_file, community_output_file, filter_threshold, sc):
    [('user_id', 'business_id'), ('39FT2Ui8KUXwmUt6hnwy-g', 'RJSFI7mxGnkIIKiJCufLkg'), ('39FT2Ui8KUXwmUt6hnwy-g', 'fThrN4tfupIGetkrz18JOg')] 
    rdd = sc.textFile(input_file).map(lambda x: tuple(x.split(",")))
    header = rdd.first()
    rdd = rdd.filter(lambda x: x != header)

    # converting a user_id to an integer to save memory
    user_to_int = rdd.map(lambda x: x[0]) \
        .distinct() \
        .zipWithIndex()
    user_to_int_dict = sc.broadcast(user_to_int.collectAsMap())

    b_to_int = rdd.map(lambda x: x[1]) \
        .distinct() \
        .zipWithIndex()
    b_to_int_dict = sc.broadcast(b_to_int.collectAsMap())

    translated_pairs = rdd.map(lambda x: (user_to_int_dict.value[x[0]], b_to_int_dict.value[x[1]])) \
        .groupByKey() \
        .mapValues(list)
    return NotImplemented

if __name__ == '__main__':
    start_time = time.time()
    sc_conf = pyspark.SparkConf() \
        .setAppName('hw4') \
        .setMaster('local[*]') \
        .set('spark.driver.memory', '4g') \
        .set('spark.executor.memory', '4g')
    
    sc = pyspark.SparkContext(conf=sc_conf)
    sc.setLogLevel("OFF")

    parser = argparse.ArgumentParser(description='A1T1')
    parser.add_argument('--filter_threshold', type=int, default=7, help='')
    parser.add_argument('--input_file', type=str, default='./ub_sample_data.csv', help='the input file')
    parser.add_argument('--betweeness_output_file', type=str, default='./betweeness.txt', help='the betweeness output file')
    parser.add_argument('--community_output_file', type=str, default='./result.txt', help='the output file contains your answers')
    args = parser.parse_args()

    main(args.input_file, args.betweeness_output_file, args.community_output_file, args.filter_threshold, sc)
    sc.stop()