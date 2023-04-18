import pyspark
import os
import argparse 

from  kmeans_util import *
NUM_CLUSTERS = 10

MAX_ITER = 1000


if __name__ == "__main__":
    

    sc_conf = pyspark.SparkConf() \
        .setAppName('hw3_task1') \
        .setMaster('local[*]') \
        .set('spark.driver.memory', '4g') \
        .set('spark.executor.memory', '4g')
    # sc = pyspark.SparkContext(conf=sc_conf)
    sc = pyspark.SparkContext.getOrCreate()
    sc.setLogLevel("OFF")

    parser = argparse.ArgumentParser(description='A1T1')
    parser.add_argument('--input_path', type=str, default='./data/test1')
    parser.add_argument('--n_cluster', type=int, default=1000)
    parser.add_argument('--out_file1', type=str, default='./outputs/cluster_results.txt') # the output file of cluster results 
    parser.add_argument('--out_file2', type=str, default='./outputs/intermediate_results.txt') # the output file of cluster results 
    args = parser.parse_args()

    chunks_names = os.listdir(args.input_path) # ['data1.txt', 'data3.txt', 'data0.txt', 'data2.txt', 'data4.txt']
    # test_data = './data/myTestData.txt' #DEBUG
    # test_data = './data/test1/data0.txt' # DEBUG
    # first reading data from the for now lets just focus on a single chunk
    rdd = sc.textFile(args.input_path) #FIXME we are going to change this to read in chunks later
    data = rdd.map(lambda x: x.split(',')) \
        .map(lambda x: (x[0], [float(num) for num in x[1:]])) # [('0', [-51.71899392687148, -16.014919500749066, -32.74523700769919, -18.521163057290334, 24.521957915324595]
    data_sample = data.sample(False, 2000/137923, seed=666) # we want to take 1/10 of the data to create cluster centers with
    data_sample = data_sample.collectAsMap()

    first_kmeans = KMeans() # creating object to store the data for our initial sample

    initKmeans(data_sample, first_kmeans, 20)
    # initKmeans(data.collectAsMap(), first_kmeans, 5) #DEBUG
    init_centroids(first_kmeans)
    for idx in range(MAX_ITER):
        assignPoints(first_kmeans)
        # print(first_kmeans.clusters) # DEBUG
        # print(' '.join(map(str, [len(cluster) for cluster in first_kmeans.clusters]))) #DEBUG
        old_centroids = first_kmeans.centroids
        new_centroids = updateCentroids(first_kmeans)
        # check if Kmeans converges
        diff = []
        for i in range(len(new_centroids)):
            diff.append(euclidean_distance(old_centroids[i], new_centroids[i]))
        print(idx)
        if sum(diff) == 0: # if we converge 
            break
        else:
            first_kmeans.centroids = new_centroids
    print()

    sc.stop()