import pyspark
import argparse
import operator
from itertools import combinations
import collections

# TODO: need to extend to handle the case for business: <user>
# user1: [business11, business12, business13, ...]
# [('4', ['102', '103', '101', '99', '97'])]


def CreateBaskets(sc: pyspark.SparkContext, inputFile: str):
    '''Creates the baskets like (user_id, <b_id>)'''
    rdd = sc.textFile(inputFile).map(
        lambda x: x.split(",")).collect()[1:]
    rdd = sc.parallelize(rdd) \
        .map(lambda x: (x[0], x[1])).reduceByKey(lambda x, y: x + ',' + y) \
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
    # TODO: Generate frequent singles
    freqSingles = GenFreqSingles(baskets, lowered_threshold)
    # TODO: create list to hold all the lists of pairs of sets
    all_sets = []
    # TODO: generate frequent doubles
    k = 2 
    current_set = GenSets(baskets, freqSingles, k, lowered_threshold)
    all_sets.append(current_set)
    max_iter = 100 
    # TODO: create loop that checks whether we can't create more freq sets or not
    while(len(current_set) and max_iter > 0):
        k+=1
        current_set = GenSets(baskets, current_set, k, lowered_threshold)
        all_sets.append(current_set)
        max_iter -= 1 
    return all_sets

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='ALT1')

    parser.add_argument('--input_file', type=str,
                        default='./data/small1.csv', help='the input file')
    parser.add_argument('--output_file', type=str,
                        default='./data/hw2/a1t1.json', help='outputfile')

    parser.add_argument('--c', type=int, default=1, help='case 1 or case 2')
    parser.add_argument('--s', type=int, default=10, help='minimum threshold')

    args = parser.parse_args()

    sc_conf = pyspark.SparkConf()
    sc_conf.setAppName('task1')
    sc_conf.setMaster('local[*]')
    sc_conf.set('spark.driver.memory', '8g')
    sc_conf.set('spark.executor.memory', '4g')
    sc = pyspark.SparkContext(conf=sc_conf)
    # sc = pyspark.SparkContext.getOrCreate()
    sc.setLogLevel("OFF")

    baskets = CreateBaskets(sc, args.input_file).map(lambda x: x[1])

    numBaskets = baskets.count()

    def SON1(x):
        baskets = list(x)
        lowered_threshold = (len(baskets) / numBaskets) * args.s
        return A_Priori(baskets, lowered_threshold)
    SonMap1 = baskets.mapPartitions(SON1).distinct().collect()
    sc.stop()
