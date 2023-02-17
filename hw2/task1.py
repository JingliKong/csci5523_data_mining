import pyspark
import argparse
import operator
from itertools import combinations

def a_priori(baskets, support_threshold): 
    solution = []
    freqSingles = FindFreqCounts(baskets, supp_threshold = support_threshold)
    solution.append(freqSingles)
    # first we try to create doubles 
    k = 2 
    # find the initial doubles from the frequent counts 
    freq_sets = FindDoubles(baskets, freqSingles, support_threshold) 
    currentSets = freq_sets 
    solution.append(freq_sets)
    while (len(currentSets)): # while we are still generating frequent pairs 
        k+=1
        # generates cantidate subset 
        new_sets = GenerateKthSet(currentSets, k)


def GenerateKthSet(basket, k): 
    # generating k-length set 
    kth_set = []
    for i in basket: 
        for j in basket: 
            temp = i.union(j)
            if (len(temp) == k): 
                kth_set.append(temp)
    return kth_set  
def Filter        

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


def FindFreqCounts(baskets: pyspark.RDD, supp_threshold: int) -> pyspark.RDD:
    '''basket: (user, [b_id])'''
    rdd = baskets.map(lambda x: x[1]) \
        .flatMap(lambda x: x) \
        .map(lambda x: (x, 1)) \
        .reduceByKey(lambda x, y: x+y) \
        .filter(lambda x: x[1] >= supp_threshold)\
        .map(lambda x: x[0])
    return rdd


def FindDoubles(baskets: pyspark.RDD, freqItems: pyspark.RDD, threshold: int, k : int):
    
    solution = []
    freqItems: list[int] = freqItems.collect()
    # contains a list of all the items we have
    baskets: list[int] = baskets.map(lambda x: x[1]).flatMap(lambda x: x).collect() 
        
    freqSets = {}
    # generate all the sets of frequent items
    pairs: list[set[str, str]] = list(combinations(freqItems, k))
    # initializing all counts to zeroes
    for p in pairs:
        freqSets[frozenset(p)] = 0

    allPairs = list(combinations(baskets, k))

    for i in allPairs:
        temp = frozenset(i)
        if temp in freqSets:
            freqSets[temp] += 1
    for k, v in freqSets.items():
        if v > threshold:
            solution.append(set(k))

    return solution



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

    rdd = None
    lowered_threshold = args.s / 2  # FIXME: I have no idea how to random sample
    support_threshold = 4  # TODO: this is just for testing remove later

    # to random sample https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.RDD.sample.html

    baskets = CreateBaskets(sc, args.input_file)

    numBaskets = baskets.count() 

    def SON_Map1(x):
        baskets = list(x)
        lowered_threshold = (len(baskets) / numBaskets) * support_threshold 
        return a_priori(baskets, lowered_threshold)
         


    # FIXME: I am not sure how to handl ethe freq threshold per basket, because currently baskets are really small so we have to make the threshold small or we get no freq pairs
    # FIXME do I get the frequent items per basket, because if so there are to few items per basket normally
    freqItems = FindFreqCounts(baskets, support_threshold)

    freq_sets = FindDoubles(baskets, freqItems, support_threshold, 2)
dsds
    sc.stop()
