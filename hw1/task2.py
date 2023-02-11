import argparse
import json
import pyspark

if __name__ == '__main__':

    sc_conf = pyspark.SparkConf()
    sc_conf.setAppName('task1')
    sc_conf.setMaster('local[*]')
    sc_conf.set('spark.driver.memory', '8g')
    sc_conf.set('spark.executor.memory', '4g')
    # sc = SparkContext(conf=sc_conf)
    sc = pyspark.SparkContext.getOrCreate()
    sc.setLogLevel("OFF")

    # # reading in parameters
    parser = argparse.ArgumentParser(description='ALT1')
    parser.add_argument('--review_file', type=str,
                        default='./backup/data/hw1/review.json', help='the input file')
    parser.add_argument('--business_file', type=str,
                        default='./backup/data/hw1/business.json', help='output file containing answers')
    parser.add_argument('--output_file', type=str,
                        default='./backup/data/hw1/a1t2.json', help='outputfile')
    parser.add_argument('--n', type=int, default=5, help='top n categories')

    args = parser.parse_args()

    # # reading in parameters
    # parser = argparse.ArgumentParser(description='ALT1')
    # parser.add_argument('--review_file', type=str,
    #                     default='./data/review.json', help='the input file')
    # parser.add_argument('--business_file', type=str,
    #                     default='./data/business.json', help='output file containing answers')
    # parser.add_argument('--output_file', type=str,
    #                     default='./data/task2_sol.json', help='outputfile')
    # parser.add_argument('--n', type=int, default=100, help='top n categories')

    # args = parser.parse_args()      

    # read in business and review files
    business_text = sc.textFile(
        args.business_file).map(lambda x: json.loads(x))
    reviews_text = sc.textFile(args.review_file).map(lambda x: json.loads(x))

    # getting the (business_ids, stars)
    reviews_rdd = reviews_text.map(lambda x: (x['business_id'], x['stars']))

    def removeWhiteSpace(ls):
        for i in range(len(ls)):
            ls[i] = ls[i].strip()
        return ls 

    # we are filtering the categories otherwise sometimes we get null from a map on the categories
    # Extracting (business_id, [categories]) after removing whitespace and splitting on commas
    business_rdd = business_text.filter(lambda x: x['categories']).map(lambda x: (x['business_id'], x['categories'])) \
        .map(lambda x: (x[0], x[1].split(','))).map(lambda x: (x[0], removeWhiteSpace(x[1])))

    # join on business_id to get business_id, (stars, [categories])
    joined = reviews_rdd.join(business_rdd)

    # now getting rid of the business_id field to just have (stars, [categories])
    stars_categories = joined.map(lambda x: (x[1][0], x[1][1])).flatMap(lambda x: [(x[0], y) for y in x[1]]).map(lambda x: (x[1], x[0]))

    # giving each category the star associated with them to get (stars, category) for each category
    # FIXME: This isn't working correctly for some reason

    # just flipping to the tuple to get the category as key (category, stars)
    # stars_categories = stars_categories.map(lambda x: (x[1], x[0]))

    # finding the average stars per category
    total_stars = stars_categories.reduceByKey(lambda x, y: x+y)

    # finding the number of counts per category review
    counts = stars_categories.map(lambda x: (
        x[0], 1)).reduceByKey(lambda x, y: x+y)

    total_avg = total_stars.join(counts).map(
        lambda x: (x[0], x[1][0]/x[1][1]))
    total_avg = total_avg.sortBy(lambda x: (-x[1], x[0]))

    result = {}
    result["result"] = total_avg.take(args.n)
    print(result["result"])
    sc.stop()  # shuts down pyspark context

    # writing to outputfile 
    f = open(args.output_file, 'w', encoding='utf-8')
    json.dump(result, f)
    f.close()