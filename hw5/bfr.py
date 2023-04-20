import pyspark
import os
import argparse 

from kmeans_util import *
from bfr_utils import * 

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
    parser.add_argument('--n_cluster', type=int, default=4)
    parser.add_argument('--out_file1', type=str, default='./outputs/cluster_results.txt') # the output file of cluster results 
    parser.add_argument('--out_file2', type=str, default='./outputs/intermediate_results.txt') # the output file of cluster results 
    args = parser.parse_args()

    chunks_names = sorted(os.listdir(args.input_path)) # ['data1.txt', 'data3.txt', 'data0.txt', 'data2.txt', 'data4.txt']
    # test_data = './data/myTestData.txt' #DEBUG
    # test_data = './data/test1/data0.txt' # DEBUG
    # first reading data from the for now lets just focus on a single chunk
    isFirstChunk = True
    DS = [] # holds information on the discard sets
    CS = []
    RS = [] 
    for chunk in chunks_names:
        chunk_path: str = f"{args.input_path}/{chunk}" 
        # 7 A.
        rdd = sc.textFile(chunk_path) 
        data = rdd.map(lambda x: x.split(',')) \
            .map(lambda x: (x[0], [float(num) for num in x[1:]])) # [('0', [-51.71899392687148, -16.014919500749066, -32.74523700769919, -18.521163057290334, 24.521957915324595]
        
        if isFirstChunk:
            data_sample = data.sample(False, 1/10, seed=103) # we want to take 1/10 of the data to create cluster centers with
            data_sample = data_sample.collectAsMap()
            # 7 B
            initial_clustering = KMeans() # creating object to store the data for our initial sample
            # runKMeans(data_sample, initial_clustering, 3 * args.n_cluster, sc) #FIXME uncomment this when we have real implementation
            runKMeans(data_sample, initial_clustering, 10, sc) #DEBUG 
            # print([len(c) for c in initial_clustering.clusters]) # debug
            bfr_centroids = initial_clustering.centroids
            # init the discard sets to hold stat information on the file data 
            # 7 C) Use the K-Means result from b to generate the DS clusters
            for i in range(len(initial_clustering.clusters)):
                cluster_features: list[float] = [data_sample[label] for label in initial_clustering.clusters[i]]
                DS.append(Discard_Set(bfr_centroids[i], cluster_features, initial_clustering.clusters[i])) # holds the labels that are in each cluster
            # 7 E: Running K-means on the rest of chunk 1 to create Compressed Sets and Retained sets for clusters who have only 1 point in them
            # filtering out data we already processed and put into Discard sets
            data = data.filter(lambda x: x[0] not in data_sample.keys())
            # clustering the rest of the data
            clustering_rest = KMeans()
            runKMeans(data.collectAsMap(), clustering_rest, args.n_cluster * 5)

            isFirstChunk = False
        print()

    sc.stop()