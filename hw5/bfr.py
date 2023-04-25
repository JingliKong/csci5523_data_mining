import pyspark
import os
import argparse 
import operator 
import itertools 
import json 

from kmeans import * 
from bfr_utils import *

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

    DS = None
    CS = None
    RS = None
    with open(args.out_file2, 'w') as f:
        f.write("round_id,nof_cluster_discard,nof_point_discard,nof_cluster_compression,nof_point_compression,nof_point_retained\n")
    for chunk_num, chunk in enumerate(chunks_names):
        chunk_path: str = f"{args.input_path}/{chunk}" 
        debug_path = "./data/myTestData.txt"
        data = sc.textFile(chunk_path) \
            .map(lambda x: x.split(',')) \
            .map(lambda x: (x[0], [float(num) for num in x[1:]]))
        all_data_dict = data.collectAsMap()
        if chunk_num == 0: # we are in our first iteration and need to initialize Discard sets
            data_sample = data.sample(False, 1.4/100, seed=103)
            data_sample_dict = data_sample.collectAsMap()
            
            sample_kmeans = KMeans(data_sample_dict, args.n_cluster * 3)
            # debug_kmeans = KMeans(data.collectAsMap(), 3)
            inliers, outliers = sample_kmeans.findDataOutliers(threshold=10)
            inliers_rdd_dict = sc.parallelize(inliers).map(lambda x: (x, data_sample_dict[x])).collectAsMap()
            process_inliers = KMeans(
                inliers_rdd_dict,
                args.n_cluster
            )
            # building initial discard sets for data that are not outliers
            DS = BuildDiscardSets(all_data_dict, process_inliers.get_cluster_data())
            if (len(outliers) != 0): # if there are outliers to process
                outliers_rdd_dict = sc.parallelize(outliers).map(lambda x: (x, data_sample_dict[x])).collectAsMap() 
                kmeans_outliers = KMeans(outliers_rdd_dict, args.n_cluster * 3)
                local_CS, local_RS = kmeans_outliers.findOutliers(threshold=1) # clusters with 1 element are put inside the RS
                CS = BuildDiscardSets(all_data_dict, local_CS)
                RS = Retained_Set(all_data_dict, [cluster[0] for cluster in local_RS if cluster])

            else: # no outliers so we have no conserved or retained sets
                CS = [] 
                RS = Retained_Set(all_data_dict, [])

        # now filtering out point we already processed in iteration 0
        data = {key: value for key, value in all_data_dict.items() if key not in data_sample_dict}
        # everyone needs to compare all their data and decide which set it goes in 
        data = assignToSet(data, DS) # checking if we can assign a point to a discard set
        data = assignToSet(data, CS) # checking the conserved sets
        # remaining points are assigned to the retained set
        RS.add_points(data)
        # now we run kmeans again on the elements in the retained sets to see if those cluster
        RS_Clusters = KMeans(RS.data_points, args.n_cluster * 3)
        new_cs, new_rs = RS_Clusters.findOutliers(threshold=1)
        CS += BuildDiscardSets(RS.data_points, new_cs)
        RS = Retained_Set(RS.data_points, [cluster[0] for cluster in new_rs if cluster]) # gets rid of extra empty lists to
 
        # now we try to merge as many conserved sets as possible
        CS = mergeSets(CS)
        # if we are at the last iteartion we have to all merge sets
        if (chunk_num == len(chunks_names) - 1):
            DS = mergeSets(DS, CS)
            CS = []
            RS.data_points = assignToSet(RS.data_points, DS)

        inter_result = [chunk_num + 1, len(DS), getNumPoints(DS), len(CS), getNumPoints(CS), len(RS.data_points)]
        with open(args.out_file2, 'a') as f:
            f.write(','.join(map(str, inter_result)))
            f.write('\n')
        print(chunk_num)
    
    # After finishing clustering we write results out 
    result = {}
    for i, one_DS in enumerate(DS):
        for data_index in one_DS.data_indices:
            result[data_index] = i
    for idx in RS.data_points:
        result[idx] = -1

    with open(args.out_file1, 'w') as f:
        json.dump(result, f)
    
        