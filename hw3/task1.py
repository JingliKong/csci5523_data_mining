import argparse
import json
import time
import pyspark
import numpy as np 

A = [13,511,586,59,791,335,625,1,564,717,549,387,57,469,145,690,206,753,790,281,184,482,192,565,591,626,144,475,515,972,871,965,82,742,340,826,543,181,691,354,994,390,96,277,815,683,221,470,465,289,824,237,695,582,609,629,695,486,561,487,677,8,36,414,137,801,492,998,983,58,99,712,527,126,725,721,117,46,222,554,283,576,503,562,596,218,185,753,798,332,980,605,485,566,696,427,915,124,182,487,595,365,908,340,826,90,557,270,750,273,481,251,348,451,795,740,124,452,421,297,641,10,512,840,449,635,163,374,338,203,960,630,75,862,962,126,979,296,190,758,500,583,17,6,345,556,899,480,207,726,782,325,488,662,414,120,575,507,2,42,486,550,142,98,596,702,409,399,317,283,806,650,267,113,915,701,231,775,247,203,382,89,568,322,496,956,206,57,104,510,112,397,21,779,610,105,548,342,253,476,525,740,527,487,65,106,362,257,216,658,806,121,594,458,985,722,667,982,315,66,797,946,802,537,110,646,631,423,125,571,726,45,62,62,470,189,155,912,40,679,428,363,84,416,738,1,347,967,690,147,929,128,871,211,314,521,257,127,910,463,578,993,639,858,239,418,411,952,828,888,189,357,282,406,646,963,252,187,294,370,165,449,457,263,767,547,333,755,972,154,484,962,280,732,956,654,213,735,550,723,928,980,921,78,602,848,836,790,773,273,996,424,541,562,807,540,349,11,12,918,897,735,44,931,471,23,234,187,463,402,506,946,646,765,780,707,980,637,80,135,264,978,42,513,566,316,260,382,343,963,524,151,676,801,632,557,784,536,922,163,798,129,588,364,3,116,986,10,599,602,631,423,572,516,7,239,734,559,968,36,390,500,348,618,407,728,289,349,878,127,846,83,880,409,652,39,648,658,289,242,593,532,425,486,785,559,385,985,968,851,593,402,683,357,585,357,667,25,304,63,101,501,602,638,11,696,499,79,558,141,473,392,499,743,762,402,126,992,920,785,586,289,465,532,374,205,820,560,656,205,27,680,582,548,831,952,402,254,86,219,969,109,338,896,724,585,283,99,978,131,640,521,177,557,940,122,199,950,487,36,790,128,76,463,876,401,877,329,510,446,831,681,39,769,788,168,964,751,936,750,786,892,192,184,783,803,889,74,935,666,452,303,274,400,880,136,172,463,899,285,397,279,107,81,201,301,168,139,76,3,324,451,860,90,515,57,727,173,15,781,933,941,65,852,961,544,467,218,655,959,64,510,244,636,108,436,78,197,718,807,652,566,994,327,650,431,936,190,742,750,349,410,125,576,108,525,735,477,249,513,362,676,792,202,191,942,67,670,725,657,354,660,10,919,932,893,694,318,258,735,55,559,877,129,652,197,633,950,929,301,576,559,223,242,133,275,561,523,697,290,972,980,167,470,371,515,158,279,286,810,625,731,412,896,890,924,757,459,478,25,211,112,258,752,606,844,465,932,471,530,718,24,975,506,907,956,352,364,832,873,25,345,350,656,666,566,82,97,377,217,600,821,813,899,542,454,931,176,227,68,368,221,955,46,292,85,776,821,653,731,722,195,818,64,690,15,407,967,813,562,228,578,176,135,384,25,872,531,321,1,858,571,128,650,526,632,158,260,368,537,242,23,365,891,835,784,193,944,808,689,108,143,583,424,307,863,977,567,589,460,992,168,517,615,862,294,710,655,846,200,425,172,131,478,455,165,800,289,635,961,350,450,273,473,322,598,468,798,74,598,749,458,108,532,192,786,977,70,94,940,271,585,552,989,865,248,110,625,116,760,679,312,708,139,475,790,387,194,695,264,213,387,776,966,499,403,190,514,477,164,96,284,378,15,79,706,121,710,261,262,310,689,823,697,573,781,574,765,590,148,907,283,495,639,829,505,276,220,304,457,419,328,524,452,715,560,734,259,528,639,688,897,305,259,713,515,381,477,406,380,839,30,195,242,651,484,865,916,498,765,913,848,169,990,540,857,973,93,803,520,773,304,449,520,686,432,289,142,160,423,643,663,964,100,115,258,815,630,896,658,181,964,538,13,454,986,973,716,529,724,620,313,56,325,527,258,105,180,100,689,421,495,634,167,143,310,192,145,341,806,335,530,172,950,654,607,635,599,760,895,852,165,997,943,242,468,858,646,526,291,451,389,521,40,474,916,299,312,876,106,587,117,75,295,437,145,51,6,586,171,103,615,946,88,640,750,72,408,274,90,781,304,470,290,245,680,202,812,551,611,266,108,24,629,985,884,447,865,131,436,392]
B = [76,181,356,716,403,338,293,393,518,164,985,229,899,371,271,499,400,403,241,24,704,964,825,949,935,546,623,292,298,132,608,638,754,120,568,309,611,566,9,698,266,996,964,592,49,12,713,969,472,723,610,135,972,650,43,901,404,471,120,378,458,174,23,13,582,876,866,659,419,54,702,305,158,128,990,709,984,377,110,140,384,55,583,794,943,200,948,35,960,634,991,387,988,723,447,356,883,704,217,715,596,783,234,609,722,205,824,949,598,363,780,631,105,29,470,468,618,299,881,206,938,744,282,134,588,914,946,721,977,577,337,394,971,378,315,677,370,234,420,895,716,632,829,572,438,622,584,330,899,437,937,486,754,956,76,878,453,504,815,551,427,619,316,666,722,90,344,558,180,437,227,409,350,215,358,844,821,850,298,216,779,599,161,869,365,42,248,187,360,197,867,893,205,285,470,803,922,773,739,39,282,567,552,721,399,730,700,583,110,425,94,656,821,881,923,75,449,400,652,572,894,441,72,511,45,368,855,797,970,369,916,605,374,664,271,669,611,378,582,776,835,209,149,21,156,487,616,372,550,460,534,952,338,688,112,740,211,36,819,668,7,21,922,325,159,65,937,551,5,352,431,640,477,138,743,269,169,711,168,369,665,12,396,859,929,678,262,69,93,327,127,337,501,690,629,957,586,971,988,352,960,745,105,283,505,468,931,50,641,181,209,680,71,479,967,916,736,735,563,462,667,805,78,449,752,295,626,267,50,860,915,843,2,481,940,113,758,376,904,892,368,289,342,720,103,108,237,631,615,72,86,451,960,366,668,988,8,137,366,78,93,935,801,155,425,430,498,987,11,958,148,128,176,602,237,196,175,230,504,649,253,899,919,700,228,83,392,36,271,447,749,532,970,846,887,828,384,493,474,732,460,147,965,972,872,867,313,323,73,504,70,594,424,534,703,484,183,26,612,880,345,620,578,935,341,291,582,452,9,675,346,629,767,267,506,664,520,644,741,82,321,482,122,473,846,195,287,993,196,718,604,617,830,511,679,27,157,768,627,191,402,969,710,364,4,38,793,117,587,473,246,963,239,623,428,303,17,268,728,418,396,160,991,826,742,665,451,674,342,415,822,339,727,936,170,334,877,426,157,352,615,533,834,412,609,903,783,959,888,151,674,705,904,581,538,707,524,802,526,990,996,57,928,988,109,227,36,139,397,794,366,406,42,31,726,931,309,858,388,949,889,81,21,686,34,215,120,310,782,16,875,478,786,165,76,388,335,612,966,451,98,83,960,252,272,919,540,987,283,13,669,815,609,463,392,54,536,213,129,229,361,924,176,738,529,276,160,454,869,737,155,146,645,757,958,302,801,635,692,875,900,954,127,201,258,509,177,2,21,622,455,859,42,744,354,61,517,440,588,60,482,489,128,837,420,195,68,890,806,797,921,713,473,782,966,351,610,295,579,584,687,670,853,544,329,539,479,939,872,269,893,94,723,833,875,510,921,260,944,181,81,855,455,797,273,510,490,709,877,913,8,199,942,930,491,257,533,309,993,644,137,219,429,604,99,574,560,625,10,903,172,818,664,434,237,423,150,149,19,818,958,888,884,839,347,221,203,608,314,66,944,711,356,154,898,981,795,72,147,263,296,629,969,194,345,845,825, 609,302,366,650,257,637,613,374,483,753,585,10,513,806,415,362,463,948,525,621,582,782,715,754,214,660,240,905,672,655,201,722,315,141,51,548,165,63,84,887,568,697,340,299,668,536,133,531,127,340,346,541,674,786,258,694,224,393,237,662,711,438,193,373,841,775,663,278,664,582,963,950,358,449,797,722,413,708,675,199,6,331,52,506,806,104,592,508,678,131,115,620,966,616,342,960,651,967,955,286,574,902,575,839,31,478,758,372,922,651,500,810,256,395,927,163,957,686,202,63,606,323,945,69,605,242,405,216,61,838,292,525,394,912,415,582,743,19,242,569,750,412,456,169,794,387,334,336,3,832,67,551,236,752,176,147,977,665,241,802,843,462,695,996,466,770,565,874,960,494,315,405,7,208,474,820,771,914,8,736,437,210,710,586,126,325,543,549,125,767,501,889,566,950,526,360,744,287,919,527,777,408,222,747,439,193,163,408,687,602,303,756,245,883,697,696,790,675,803,937,119,130,693,463,309,96,176,733,316,593,499,747,108,67,304,450,709,617,693,346,803,471,40,151,835,522,622,279,548,238,977,483,376,833,751,857,277,741,595,249,611,799,333,864,636,693,396,647,75,892,846,583,191,434]
M = 25

def MinHash(rdd: pyspark.rdd.PipelinedRDD) -> pyspark.rdd.PipelinedRDD: 
    '''rdd: (business_id, [0, 1, 0, ...])'''

    observations: list[int] = list(rdd[1]) 
    signatures = np.zeros(M, dtype=int)
    # we have M signatures 
    for i in range(M): 
        hashes = []
        nonzero_entries = np.nonzero(observations)
        for x in range(len(nonzero_entries)): 
            val = (A[i]*observations[x] + B[i]) % M
            # print(val)
            hashes.append(val)
        signatures[i] = min(hashes)

    return (rdd[0], signatures) 

def main(input_file, output_file, jac_thr, n_bands, n_rows, sc):

    """ You need to write your own code """

    rdd = sc.textFile("./data/train_review.json").map(lambda line: json.loads(line))
    
    user_rdd = rdd.map(lambda x: x['user_id']).distinct()
    # every worker node know how many total users we have so we can intialize the sparse matrix 
    num_users = sc.broadcast(user_rdd.count())  

    # broadcasting the lookup table to know which index of the vector corresponds to each user 
    user_translation = user_rdd.zipWithIndex().map(lambda x: (x[0], x[1])).collectAsMap()
    user_translation = sc.broadcast(user_translation)

    # (business_id, user_id)
    pairs = rdd.map(lambda x: (x['business_id'], x['user_id'])) \
        .groupByKey() 
    
    def CreateUserVector(x):
        # recall the list of users is currently a pyspark iterable object 
        users = list(x[1])
        # initialzing an array of 0 the size of the number of users  
        vector_rep = np.zeros(num_users.value)
        for u in users: 
            index = user_translation.value[u]
            vector_rep[index] = 1  
        # (business_id, [0, 1, 0, ...])
        return (x[0], vector_rep)

    # creating [('b_id', array([1., 0., 0., ..., 0., 0., 0.]))]
    pairs = pairs.map(lambda x: CreateUserVector(x))
    # [('eMiN8nm70jjKg8izikVWDA', array([89, 92, 42, 75, 94, 73, 18, 94, 82, 81, 34, 16])
    signatures = pairs.map(lambda x: MinHash(x))

    
    print()



if __name__ == '__main__':
    start_time = time.time()
    sc_conf = pyspark.SparkConf() \
        .setAppName('hw3_task1') \
        .setMaster('local[*]') \
        .set('spark.driver.memory', '4g') \
        .set('spark.executor.memory', '4g')
    sc = pyspark.SparkContext(conf=sc_conf)
    sc.setLogLevel("OFF")

    parser = argparse.ArgumentParser(description='A1T1')
    parser.add_argument('--input_file', type=str, default='./data/train_review.json')
    parser.add_argument('--output_file', type=str, default='./outputs/task1.out')
    parser.add_argument('--time_file', type=str, default='./outputs/task1.time')
    parser.add_argument('--threshold', type=float, default=0.1)
    parser.add_argument('--n_bands', type=int, default=50)
    parser.add_argument('--n_rows', type=int, default=2)
    args = parser.parse_args()

    main(args.input_file, args.output_file, args.threshold, args.n_bands, args.n_rows, sc)
    sc.stop()

    # log time
    with open(args.time_file, 'w') as outfile:
        json.dump({'time': time.time() - start_time}, outfile)
    print('The run time is: ', (time.time() - start_time))