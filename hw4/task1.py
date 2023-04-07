import argparse
import pyspark 
from graphframes import *

# used for reading in csv file 
from pyspark.sql.types import StructType, StructField, StringType 

schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("business_id", StringType(), True)
])

# running: spark-submit --packages graphframes:graphframes:0.8.2-spark3.2-s_2.12 task1.py
def main(input_file, output_file, threshold, sc, sqlContext):

    df = sqlContext.read.format("csv") \
        .option("header", "true") \
        .schema(schema) \
        .load(input_file) 
        
    # looks like via df.show()
    # +--------------------+--------------------+
    # |             user_id|         business_id|
    # +--------------------+--------------------+
    # |39FT2Ui8KUXwmUt6h...|RJSFI7mxGnkIIKiJC...|
    # |39FT2Ui8KUXwmUt6h...|fThrN4tfupIGetkrz...|
    # |39FT2Ui8KUXwmUt6h...|mvLdgkwBzqllHWHwS...|
    # |39FT2Ui8KUXwmUt6h...|uW6UHfONAmm8QttPk...|
    # |39FT2Ui8KUXwmUt6h...|T70pMoTP008qYLsIv...|
    # +--------------------+--------------------+
        
    # easier for me to work with rdds 
    rdd = df.rdd 
    # at this point we need to give each user and each business and id because it takes up less memory 
    # goal is to map all the distinct users to an integer id  
    user_to_int = rdd.map(lambda x: x[0]) \
        .distinct() \
        .zipWithIndex()
    user_to_int_dict = user_to_int.collectAsMap() 
    user_to_int_dict = sc.broadcast(user_to_int_dict)
    # user_count = user_to_int.count()
    
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
        intersection = b1 & b2
        if (len(intersection) >= threshold):
            return True
        return False
    
    # creating pairs like ((0, {0, 1, 2, 3, 4, 5, 6, 7, 8, ...}), (1, {2, 10, 21, 32, 57, 64, 79, 92, 93, ...})) etc so I can check if they have an edge or not
    # the filter remove duplicate pairs
    all_pairs = translated_pairs.cartesian(translated_pairs) \
        .filter(lambda x: x[0][0] != x[1][0]) \
        .filter(isEdge)
    # edges = all_pairs.map(lambda x: (x[0][0], x[1][0])) \
    #     .map(frozenset) \
    #     .distinct() \
    #     .map(tuple)
    edges = all_pairs.map(lambda x: (x[0][0], x[1][0])) \
        .distinct()
    nodes = edges.flatMap(lambda x: x).distinct()
    print("number of edges in the graph:", edges.count())
    print("number of nodes in the graph:", nodes.count())
    # now we need to create the dictionaries to convert the integers backs into their string ids 
    int_to_user = user_to_int.map(lambda x: (x[1], x[0])).collectAsMap()
    int_to_user = sc.broadcast(int_to_user)
    # note we don't need to translate the businesses back into their strings because we only care about nodes and edges 
    # now we need to convert our nodes and edges back to a dataframe so we can use GraphFrame 
    # +--------------------+--------------------+
    # |                 src|                 dst|
    # +--------------------+--------------------+
    # |39FT2Ui8KUXwmUt6h...|0FVcoJko1kfZCrJRf...|
    # |39FT2Ui8KUXwmUt6h...|JM0GL6Dx4EuZ1mprL...|
    # |39FT2Ui8KUXwmUt6h...|bSUS0YcvS7UelmHvC...|
    # |39FT2Ui8KUXwmUt6h...|DKolrsBSwMTpTJL22...|
    # +--------------------+--------------------+
    graphEdges = edges.map(lambda x: (int_to_user.value[x[0]], int_to_user.value[x[1]])).toDF(["src", "dst"]) # edge between user1 and user2
    # +--------------------+
    # |             user_id|
    # +--------------------+
    # |39FT2Ui8KUXwmUt6h...|
    # |0FVcoJko1kfZCrJRf...|
    # |JM0GL6Dx4EuZ1mprL...|
    # +--------------------+    
    graphNodes = nodes.map(lambda x: int_to_user.value[x]).map(lambda x: (x,)).toDF(["id"])
    
    # creating graph to run labelPropgation on
    graph = GraphFrame(graphNodes, graphEdges)
    communities = graph.labelPropagation(maxIter=5)
    # communities.show(4)
    # now we have a dataframe that looks like: 
    # +--------------------+------------+
    # |                  id|       label|
    # +--------------------+------------+
    # |oegRUjhGbP62M18Wy...|678604832768|
    # |gH0dJQhyKUOVCKQA6...|146028888064|
    # |2quguRdKBzul3GpRi...|627065225216|
    # |DPtOaWemjBPvFiZJB...|867583393794|
    # +--------------------+------------+
    # And we want to prepare the data to write to file where we have each line reprsent a community with the users that are in the community
    # 1) swap the ids and labels that way we get (community_id, user_id)
    # 2) groupByKey so we get all the users that are in the community 
    # 3) sort the user_ids lexigraphically (in alphabetic order)
    # 4) sort by size of community (the length of the corresponding user list)
    # make sure to convert back to an rdd
    processed = communities.rdd.map(lambda x: (x[1], x[0])) \
        .groupByKey().mapValues(list) \
        .map(lambda x: sorted(x[1])) \
        .sortBy(lambda x: (len(x), x[0]))
    # note the final .sortBy(lambda x: (len(x), x[0])) sorts the rdd be based on these priorities
    # 1. The number of users who are in the cluster
    # 2. The lexographic value of the cluster_id which I'm pretty sure is a string
    final_result = processed.collect()
    with open(output_file, 'w') as f: 
        for r in final_result:
            f.write(f'{r}\n')
 


if __name__ == '__main__':


    conf = pyspark.SparkConf().setAppName("Task1").setMaster("local[*]")
    sc = pyspark.SparkContext(conf = conf)
    sc.setLogLevel("ERROR")

    sqlContext = pyspark.sql.SparkSession.builder \
        .appName("Task1").master("local[*]").getOrCreate()    
    
    parser = argparse.ArgumentParser(description='A1T1')
    parser.add_argument('--filter_threshold', type=int, default=7)
    parser.add_argument('--input_file', type=str, default='./data/ub_sample_data.csv')
    parser.add_argument('--community_output_file', type=str, default='./outputs/task1.out')
    
    args = parser.parse_args()

    main(args.input_file, args.community_output_file, args.filter_threshold, sc, sqlContext)

    sc.stop() 