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
        .map(lambda x: tuple(x)) \
    # Then we can use those edges to extract the nodes to get
    # [0, 1, 3, 5, 10, 14, 16, 19, 20, 23]
    nodes = edges.flatMap(lambda x: x).distinct()
    print("number of edges in the graph:", edges.count())
    print("number of nodes in the graph:", nodes.count())
    # now we need to create the dictionaries to convert the integers backs into their string ids 
    int_to_user = user_to_int.map(lambda x: (x[1], x[0])).collectAsMap()
    int_to_user = sc.broadcast(int_to_user) 

    edges = edges.collect()
    nodes = nodes.collect()
    # building graph for all nodes
    graph = graph_utils.createGraph(nodes, edges) # this is just a dictionary
    myGraph = graph_utils.MyGraph(graph) # this uses the dictionary to create a graph I can processs
    trees = sc.parallelize(nodes).map(lambda x: myGraph.createTree(x))
    all_betweeness = trees.map(lambda x: graph_utils.calcWeightsForGN(myGraph, x)) \
        .flatMap(lambda x: x.items()) \
        .reduceByKey(lambda a, b: a+b) \
        .mapValues(lambda x: x/2) \
        .map(lambda x: (tuple(x[0]), x[1])) \
        .map(lambda x: ((int_to_user.value[x[0][0]], int_to_user.value[x[0][1]]), x[1]))
    final_results = all_betweeness.sortBy(lambda x: x[1], ascending = False) \
        .sortBy(lambda x: (-x[1], x[0])) \
        .collect()
    with open(betweeness_output_file, 'w') as f:
        for r in final_results:
            f.write(f'({r[0][0]}, {r[0][1]}), {r[1]}\n')
    # starting part 2 Community Detection
    M = len(myGraph.edges)
    A = myGraph.gdict
    best_cut = frozenset((user_to_int_dict.value[final_results[0][0][0]], user_to_int_dict.value[final_results[0][0][1]]))
    current_graph = graph_utils.MyGraph(myGraph.gdict).removeEdge(best_cut) # best_community starts out as the first cut we make because thats all we have for now 
    modularity = graph_utils.findModularity(A, current_graph, M) # (community, modularity score)
    best_modularity = modularity
    
    for i in range(len(myGraph.edges)):
        trees = sc.parallelize(current_graph.nodes).map(lambda x: current_graph.createTree(x))
        largest_betweenness = trees.map(lambda x: graph_utils.calcWeightsForGN(current_graph, x)) \
            .flatMap(lambda x: x.items()) \
            .reduceByKey(lambda a, b: a+b) \
            .mapValues(lambda x: x/2) \
            .map(lambda x: (tuple(x[0]), x[1])) \
            .max(key=lambda x: x[1]) # give you the best cut
        next_graph = graph_utils.MyGraph(current_graph.gdict).removeEdge(largest_betweenness[0]) # the edge to cut is the first element of the tuple
        modularity = graph_utils.findModularity(A, next_graph, M)
        if (modularity[1] > best_modularity[1]):
            best_modularity = modularity
        if (len(next_graph.edges) == 0):
            break
        current_graph = next_graph
        # print(f'i: {i} {best_modularity[1]}')
    best_communities = sc.parallelize(best_modularity[0])
    def translate_to_user(x):
        community = list(x)
        translated = []
        for user in community:
            translated.append(int_to_user.value[user])
        return translated
    best_communities = best_communities.map(lambda x: translate_to_user(x)) \
        .map(lambda x: sorted(x)) \
        .sortBy(lambda x: (len(x), x[0])) \
        .collect()

    with open(community_output_file, 'w') as f:
        for c in best_communities:
            f.write(f'{c}\n')
            
        
    # print(best_modularity)
    # print()
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
    parser.add_argument('--betweenness_output_file', type=str, default='./betweeness.txt', help='the betweeness output file')
    parser.add_argument('--community_output_file', type=str, default='./result.txt', help='the output file contains your answers')
    args = parser.parse_args()

    main(args.input_file, args.betweenness_output_file, args.community_output_file, args.filter_threshold, sc)
    end_time = time.time()
    print(f"runtime: {end_time - start_time}")
    sc.stop()