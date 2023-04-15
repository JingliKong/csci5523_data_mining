import argparse
import json
import time
import pyspark
from graphframes import *

# used for reading in csv file 
from pyspark.sql.types import StructType, StructField, StringType 

schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("business_id", StringType(), True)
])

def main(filter_threshold, input_file, output_file, sc, sqlContext):

    # reading in file 
    df = sqlContext.read.format("csv") \
        .option("header", "true") \
        .schema(schema) \
        .load(input_file) 
    # converting to rdd
    rdd = df.rdd 
    # converting a user_id to an integer to save memory
    user_to_int = rdd.map(lambda x: x[0]) \
        .distinct() \
        .zipWithIndex()
    user_to_int_dict = user_to_int.collectAsMap() 
    # making sure every node gets broadcasted the translation from user_id to int
    user_to_int_dict = sc.broadcast(user_to_int_dict)
    # doing the same for business_ids so I can save memory
    b_to_int = rdd.map(lambda x: x[1]) \
        .distinct() \
        .zipWithIndex()
    b_to_int_dict = b_to_int.collectAsMap()
    b_to_int_dict = sc.broadcast(b_to_int_dict)

    # going from (user, b) -> (int_of_user, int_of_b) -> (user, set(b)) we have integers rather than full ids 
    # then 
    translated_pairs = rdd.map(lambda x: (user_to_int_dict.value[x[0]], b_to_int_dict.value[x[1]])) \
        .groupByKey() \
        .mapValues(set)
            
    def isEdge(x):
        '''
        If two users have >= threshold # of businesses in common they have an edge connecting them
        x: is a pair of ((user, set(businesess)), (user, set(businesess)) )
        '''
        # businesses which first user has rated 
        b1 = x[0][1] 
        # second user 
        b2 = x[1][1]
        intersection = b1.intersection(b2)
        if (len(intersection) >= filter_threshold):
            return True
        return False
    # creating pairs like ((0, {0, 1, 2, 3, 4, 5, 6, 7, 8, ...}), (1, {2, 10, 21, 32, 57, 64, 79, 92, 93, ...})) etc so I can check if they have an edge or not
    # the filter remove duplicate pairs after the cartesian
    # after which we check if there is an edge between two pairs of nodes 
    all_pairs = translated_pairs.cartesian(translated_pairs) \
        .filter(lambda x: x[0][0] != x[1][0]) \
        .filter(isEdge)

    # note above I am filtering By my isEdge so we get pairs of edges like
    #  [(0, 1), (0, 3), (0, 5), (0, 10), (0, 14), (0, 16), (0, 19), (0, 20), (0, 23), (0, 28)]
    edges = all_pairs.map(lambda x: (x[0][0], x[1][0])) \
        .distinct() \
        .map(lambda x: tuple(x))
    # Then we can use those edges to extract the nodes to get
    # [0, 1, 3, 5, 10, 14, 16, 19, 20, 23]
    nodes = edges.flatMap(lambda x: x).distinct()
    print("number of edges in the graph:", edges.count())
    print("number of nodes in the graph:", nodes.count())
    # now we need to create the dictionaries to convert the integers backs into their string ids 
    int_to_user = user_to_int.map(lambda x: (x[1], x[0])).collectAsMap()
    int_to_user = sc.broadcast(int_to_user)    
    # turns my of tuples into a relation table of src and dst nodes 
    graphEdges = edges.map(lambda x: (int_to_user.value[x[0]], int_to_user.value[x[1]])).toDF(["src", "dst"]) # edge between user1 and user2
    # nodes are just a table with 1 col of nodes
    graphNodes = nodes.map(lambda x: int_to_user.value[x]).map(lambda x: (x,)).toDF(["id"])

    graph = GraphFrame(graphNodes, graphEdges)
    communities = graph.labelPropagation(maxIter=5)
    # recall labelPropagation gives us a community_id and node so we groupby key to create the communities and all we want are the communities without the id so I do map x[1]
    communities = communities.rdd.map(lambda x: (x[1], x[0])) \
        .groupByKey().mapValues(list) \
        .map(lambda x: x[1]) \
        .collect()
    # example of identified communities
    # communities = [['23y0Nv9FFWn_3UWudpnFMA'],['3Vd_ATdvvuVVgn_YCpz8fw'], ['0KhRPd66BZGHCtsb9mGh_g', '5fQ9P6kbQM_E0dx8DL6JWA' ]]
    # for i in communities:
    #     print(i)

    """ code for saving the output to file in the correct format """
    resultDict = {}
    for community in communities:
        community = list(map(lambda userId: "'" + userId + "'", sorted(community)))
        community = ", ".join(community)

        if len(community) not in resultDict:
            resultDict[len(community)] = []
        resultDict[len(community)].append(community)

    results = list(resultDict.items())
    results.sort(key = lambda pair: pair[0])

    output = open(output_file, "w")

    for result in results:
        resultList = sorted(result[1])
        for community in resultList:
            output.write(community + "\n")
    output.close()
    
if __name__ == '__main__':
    start_time = time.time()
    sc_conf = pyspark.SparkConf() \
        .setAppName('hw4') \
        .setMaster('local[*]') \
        .set('spark.driver.memory', '4g') \
        .set('spark.executor.memory', '4g')
    
    sc = pyspark.SparkContext(conf=sc_conf)
    sc.setLogLevel("OFF")
    sqlContext = pyspark.sql.SparkSession.builder \
        .appName("Task1").master("local[*]").getOrCreate()   
    parser = argparse.ArgumentParser(description='A1T1')
    parser.add_argument('--filter_threshold', type=int, default=7, help='')
    parser.add_argument('--input_file', type=str, default='./ub_sample_data.csv', help='the input file')
    parser.add_argument('--community_output_file', type=str, default='./result.txt', help='the output file contains your answers')
    args = parser.parse_args()
    
    main(args.filter_threshold, args.input_file, args.community_output_file, sc, sqlContext)
    end_time = time.time()
    print(f"runtime: {end_time - start_time}")
    sc.stop()