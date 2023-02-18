import pyspark
import argparse
import json
from itertools import combinations
import collections
import time

# TODO: need to extend to handle the case for business: <user>
# user1: [business11, business12, business13, ...]
# [('4', ['102', '103', '101', '99', '97'])]


def CreateBaskets(sc: pyspark.SparkContext, inputFile: str, case: int):
    rdd = None 
    if case == 1:
        '''Creates the baskets like (user_id, <b_id>)'''
        rdd = sc.textFile(inputFile).map(
            lambda x: x.split(",")).collect()[1:]
        rdd = sc.parallelize(rdd) \
            .map(lambda x: (x[0], x[1])).reduceByKey(lambda x, y: x + ',' + y) \
            .map(lambda x: (x[0], x[1].split(',')))
    elif case == 2:
        '''Creates the baskets like (user_id, <b_id>)'''
        rdd = sc.textFile(inputFile).map(
            lambda x: x.split(",")).collect()[1:]
        rdd = sc.parallelize(rdd) \
            .map(lambda x: (x[1], x[0])).reduceByKey(lambda x, y: x + ',' + y) \
            .map(lambda x: (x[0], x[1].split(',')))
    return rdd


def GenFreqSingles(baskets: list, support: int) -> list:
    sol = []
    counts = {}
    for basket in baskets:
        for item in basket:
            if item not in counts:
                counts[item] = 1
            elif (item in counts):
                counts[item] += 1
    for k, v in counts.items():
        if (v > support):
            sol.append({k})
    return sol


def GenSets(baskets: list[list], freqItems: list, k: int, support: int):
    sol = []
    counts = collections.defaultdict(int)

    for i in freqItems:
        for j in freqItems:
            temp = i.union(j)
            if (len(temp) == k):
                counts[frozenset(temp)] = 0

    all_sets = []
    for i in range(len(baskets)):
        all_sets.append(list(combinations(baskets[i], k)))

    # print(counts)
    for val in baskets:
        for i in range(len(val)):
            for j in range(len(val)):
                temp = frozenset({val[i], val[j]})
                if temp in counts:
                    counts[temp] += 1

    for k, v in counts.items():
        if v >= support:
            sol.append(set(k))

    return sol


def A_Priori(baskets, lowered_threshold):
    all_sets = []
    # TODO: Generate frequent singles
    freqSingles = GenFreqSingles(baskets, lowered_threshold)
    # appending singles to the sol 
    all_sets.append(freqSingles)
    
    k = 2
    current_set = GenSets(baskets, freqSingles, k, lowered_threshold)
    all_sets.append(current_set)
    max_iter = 100
    # TODO: create loop that checks whether we can't create more freq sets or not
    while(len(current_set) != 0 and max_iter > 0):
        k += 1
        current_set = GenSets(baskets, current_set, k, lowered_threshold)
        all_sets.append(current_set)
        max_iter -= 1
    return all_sets


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='ALT1')

    parser.add_argument('--input_file', type=str,
                        default='./data/small1.csv', help='the input file')
    parser.add_argument('--output_file', type=str,
                        default='./data/a1t1.json', help='outputfile')

    parser.add_argument('--c', type=int, default=2, help='case 1 or case 2')
    parser.add_argument('--s', type=int, default=4, help='minimum threshold')

    args = parser.parse_args()

    sc_conf = pyspark.SparkConf()
    sc_conf.setAppName('task1')
    sc_conf.setMaster('local[*]')
    sc_conf.set('spark.driver.memory', '8g')
    sc_conf.set('spark.executor.memory', '4g')
    sc = pyspark.SparkContext(conf=sc_conf)
    # sc = pyspark.SparkContext.getOrCreate()
    sc.setLogLevel("OFF")

    start_time: float = time.time() 

    baskets = CreateBaskets(sc, args.input_file, args.c).map(lambda x: x[1])

    numBaskets = baskets.count()

    def SON1(x):
        baskets = list(x)
        lowered_threshold = (len(baskets) / numBaskets) * args.s
        return A_Priori(baskets, lowered_threshold)
    # SON's algorithm first map step
    # Generates frequent cantidates [{'101', '102'}, {'97', '102'}, {'102', '98'}, {'97', '101'}, {'101', '98'}, {'97', '98'}]
    SonMap1 = baskets.mapPartitions(SON1)

    # First reduce function, but honestly the 1st reduce, second map and last reduce are all done here
    # outputs a sest of k,v pairs (C,v) where C is one of the cantidate sets and v is the support for that itemset
    '''
    0: (('102', '98'), 3)
    1: (('103', '98'), 1)
    2: (('105', '98'), 1)
    3: (('102', '99'), 1)
    4: (('99', '101'), 3)
    '''
    cantidates = SonMap1.flatMap(lambda x: x).map(lambda x: tuple(x))
    
    freq_itemsets = cantidates.map(lambda x: (x, 1)) \
        .reduceByKey(lambda x, y: x+y) \
        .filter(lambda x: x[1] >= args.s) \
        .map(lambda x: x[0])
    total_time : float = time.time() - start_time 

    sol = {}
    sol["Cantidates"] = cantidates.collect()
    sol["Frequent Itemsets"] = freq_itemsets.collect() 
    sol["Runtime"] = total_time
    
    sc.stop()
    
    # writing to outputfile 
    f = open(args.output_file, 'w', encoding='utf-8')
    # json.dump(sol, f, separators= (",", "\n")) # more readable 
    json.dump(sol, f)
    f.close()
