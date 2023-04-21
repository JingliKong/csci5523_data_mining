import pyspark
import os
import argparse 
import operator 

from kmeans_util import *
from bfr_utils import * 

NUM_CLUSTERS = 10

MAX_ITER = 1000

def combine_cluster_lists(list1, list2):
    # Zip the elements of the two input lists and concatenate them
    concatenated_list = [a + b for a, b in zip(list1, list2)]
    # Filter out any empty elements from the concatenated list
    filtered_list = [list(filter(None, sublist)) for sublist in concatenated_list]
    return filtered_list

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
    parser.add_argument('--n_cluster', type=int, default=10)
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
            #Below line is used for DEBUG
            # data_sample = sc.textFile("./data/myTestData.txt").map(lambda x: x.split(',')).map(lambda x: (x[0], [float(num) for num in x[1:]])) 
            data_sample = data.sample(False, 1/10, seed=103) # we want to take 1/10 of the data to create cluster centers with
            data_sample_dict = data_sample.collectAsMap()

            features = list(data_sample_dict.values())
            labels = list(data_sample_dict.keys())
            dim = len(features[0])
            clusters = []
            centroids = sc.broadcast(initCentroids(features, args.n_cluster))
            current_iter = 0
            prev_cluster = None
            while (current_iter < MAX_ITER):
                clusters = data_sample.map(lambda x: assignPoints(x, centroids)) \
                    .reduce(lambda x, y: combine_cluster_lists(x,y))
                cluster_lengths = [len(cluster) for cluster in clusters] # stores how many features are in each cluster used for finding the centroid later

                old_centroids = centroids
                # new_centroids = updateCentroids(clusters, features, labels, dim)
                new_centroids = sc.parallelize(clusters).zipWithIndex().map(lambda x: (x[1], x[0])) \
                    .flatMap(lambda x: [(x[0], label) for label in x[1]]) \
                    .map(lambda x: (x[0], data_sample_dict[x[1]])) \
                    .flatMap(lambda x: [((x[0], i), value) for i, value in enumerate(x[1])]) \
                    .reduceByKey(operator.add) \
                    .map(lambda x: (x[0], x[1]/cluster_lengths[x[0][0]])) \
                    .map(lambda x: (x[0][0], (x[0][1], x[1]))) \
                    .groupByKey() \
                    .mapValues(list) \
                    .map(lambda x: createCentroid(x, dim)) \
                    .sortBy(lambda x: x[0]) \
                    .map(lambda x: x[1]) \
                    .collect()
                # Compare the current clusters with the previous clusters
                if prev_cluster is not None and clusters == prev_clusters:
                    break
                else:
                    prev_clusters = clusters  # Update prev_clusters with the current clusters
                    centroids = sc.broadcast(new_centroids)
                # print(current_iter)
                current_iter += 1    

            for i in range(len(clusters)):
                cluster_features: list[float] = [data_sample[label] for label in clusters[i]]
                DS.append(Discard_Set(centroids[i], cluster_features, clusters[i])) # holds the labels that are in each cluster
            # 7 E: Running K-means on the rest of chunk 1 to create Compressed Sets and Retained sets for clusters who have only 1 point in them
            # filtering out data we already processed and put into Discard sets
            data = data.filter(lambda x: x[0] not in data_sample.keys())

            # clustering the rest of the data 

            isFirstChunk = False
        print()

    sc.stop()
