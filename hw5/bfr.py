import pyspark
import os
import argparse 
import operator 

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
    parser.add_argument('--n_cluster', type=int, default=3)
    parser.add_argument('--out_file1', type=str, default='./outputs/cluster_results.txt') # the output file of cluster results 
    parser.add_argument('--out_file2', type=str, default='./outputs/intermediate_results.txt') # the output file of cluster results 
    args = parser.parse_args()

    chunks_names = sorted(os.listdir(args.input_path)) # ['data1.txt', 'data3.txt', 'data0.txt', 'data2.txt', 'data4.txt']
    # test_data = './data/myTestData.txt' #DEBUG
    # test_data = './data/test1/data0.txt' # DEBUG
    # first reading data from the for now lets just focus on a single chunk
    isFirstChunk = True
    DS = [] # holds information on the discard sets
    CS = [] # compressed set 
    RS = [] # retained sets
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
            # labels = list(data_sample_dict.keys())
            dim = len(features[0]) # dimension of each feature/centroid

            clusters, centroids = runKmeans(data_sample, data_sample_dict, features, dim, 3 * args.n_cluster, sc)

            # after we run kmeans the first time we have a number of outliers and inliers the inliers get processed again to get our Discard sets
            inliers, outliers = findOutliers(clusters)
            # at this point we know inliers should be used to create Discard sets
            inlier_labels = sc.parallelize(inliers).flatMap(lambda x: x).collect()

            create_DS_data = data_sample.filter(lambda x: x[0] in inlier_labels) # creating initial DS sets with inliers from the sameple data
            create_DS_data_dict = create_DS_data.collectAsMap()
            features = list(create_DS_data_dict.values())
            clusters, centroids = runKmeans(create_DS_data, create_DS_data_dict, features, dim, args.n_cluster * 5, sc)
            # creating our initial discard sets
            # 7 E: The initialization of DS has finished, so far, you have K clusters in DS.
            DS = sc.parallelize(clusters).zipWithIndex().map(lambda x: createDSList(x, create_DS_data_dict, centroids)).collect()
            # run kmeans again on the create_DS_data 
            # for i in range(len(clusters)):
            #     cluster_features: list[list[float]] = [data_sample[label] for label in clusters[i]]
            #     DS.append(Discard_Set(centroids[i], cluster_features, clusters[i])) # holds the labels that are in each cluster
            # 7 E: Running K-means on the rest of chunk 1 to create Compressed Sets and Retained sets for clusters who have only 1 point in them
            # filtering out data we already processed and put into Discard sets
            processed_labels = list(create_DS_data_dict.keys())

            data = data.filter(lambda x: x[0] not in processed_labels)
            data_dict = data.collectAsMap()
            features = list(data_dict.values())
            clusters, centroids = runKmeans(data, data_dict, features, dim, args.n_cluster * 5, sc)

            # cluster_lengths = [len(cluster) for cluster in clusters] #DEBUG
            # print(cluster_lengths) #DEBUG
            temp = sc.parallelize(clusters).zipWithIndex()
            current_CS = temp.filter(lambda x: len(x[0]) > 1)
            current_RS = temp.filter(lambda x: len(x[0]) == 1)
            
            # clustering the rest of the data 

            isFirstChunk = False
        print()

    sc.stop()
