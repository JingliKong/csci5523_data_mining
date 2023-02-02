import pyspark 
import argparse 
import json 

# run: 
# python task1.py -- input_file <input_file> --output_file <output_file> --stopwords <stopwords> --y <y> --m <m> --n <n>
if __name__ == '__main__':

    sc_conf = pyspark.SparkConf()
    sc_conf.setAppName('task1')  
    sc_conf.setMaster('local[*]') 
    sc_conf.set('spark.driver.memory', '8g')
    sc_conf.set('spark.executor.memory', '4g')
    sc = pyspark.SparkContext(conf=sc_conf)
    sc.setLogLevel("OFF")

    # reading in parameters 
    parser = argparse.ArgumentParser(description='ALT1')
    parser.add_argument('--input_file', type=str, default='./data/review.json', help='the input file')
    parser.add_argument('--output_file', type=str, default='./data/alt1.json', help='output file containing answers')
    parser.add_argument('--stopwords', type=str, default='./data/stopwords', help='stopwords')
    parser.add_argument('--y', type=int, default=2014, help='year')
    parser.add_argument('--m', type=int, default=18, help='top m users')
    parser.add_argument('--n', type=int, default=18, help='top n frequency words')
    args = parser.parse_args()

    print(f'input file: {args.input_file}')
    print(f'output file: {args.output_file}')
    print(f'year {args.y}')
    print(f'top {args.m} users')
    print(f'top {args.n} frequency words')
    
    # params = {'years': args.y, 'input_file': args.input_file, 'output_file': args.output_file}
    
    solutions = {'A': None, 'B': None, 'C': None, 'D': None, 'E': None} 

    # solution fill in through computation 
    total_num_review : int = 0 

    # read in file and create rdd 
    lines = sc.textFile(args.input_file)

    #processing the json file into rdd 
    rdd = lines.map(lambda line: json.loads(line))

    # selecting review_id column from json file
    # NOTE: This is still a rdd but looks like a python list of the review_ids 
    # e.g., ['-I5umRTkhw15RqpKMl_o1Q', 'qlXw1JQ0UodW7qrmVgwCXw', '1wVA2-vQIuW_ClmXkDxqMQ', 'rEITo90tpyKmEfNDp3Ou3A', 'IPw8yWiyqnfBzzWmypUHgg']
    ids = rdd.map(lambda x: (x['review_id']))  
    
    # get all the distinct ids -> get all the distinct ids into a list -> then count 
    unique_ids = sc.parallelize(ids.distinct().collect()).count() 
    # 4.1.1 A  
    solutions['A'] = unique_ids # solution for question A

    # Starting 4.1.1 B

    # first map review ids and dates into list of key value pairs. e.g, (ids, dates)  
    ids_and_dates : list = rdd.map(lambda x: (x['review_id'], x['date'])).collect() # recall when we collect we return an actual value not just rdd 
    
    

    #filtering by year returns list of (ids, dates) which correspond to the given year  
    filter_by_year = sc.parallelize(ids_and_dates) # just turns the ids_and_dates list back into an rdd 
    print(f'years: {args.y}') #TODO remove this after debugging 
    filter_by_year : list = filter_by_year.filter(lambda x: x[1][0:4] == args.y).collect() #FIXME: when putting a constant like 2017 this works as expected but using args.y doesn't find anything 

    # then all we have left to do is make sure we only return unique reviews by checking their ids 
    filter_by_year : int = sc.parallelize(sc.parallelize(filter_by_year).distinct().collect()).count()
    solutions['B'] = filter_by_year 
     
    # 4.1.1 C 

    # select user_id from from rdd  
    users = rdd.map(lambda x: x['user_id'])
    # only grab the unique user ids 
    unique_users = sc.parallelize(users.distinct().collect()).count()
    solutions['C'] = unique_users 

    # 4.1.1 A Total number of reviews   
    print(f'A: ', solutions['A'])   

    # 4.1.1 B Total number of reviews in a given year    
    print(f'B: ', solutions['B'])

    # 4.1.1 C unique users 
    print(f'C: ', solutions['C'])

    # 4.1.1 D Top m users who have the largest number of reviews and its count

    # 4.1.1 E Top n frequent words in the review text. 

