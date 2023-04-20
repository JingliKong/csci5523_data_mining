import math

MAX_ITER = 1000
class KMeans: # object to hold information for kmeans
    # Data set to be clustered                      
    ndata    = 0                     # count of data
    dim      = 0                     # dimension of features for data
    features = []                    # pointers to individual features
    assigns  = []                    # cluster to which data is assigned
    labels   = []                    # label/index for data 

    # information on algorithm work
    num_clusters = 0
    max_iter = 1000
    # cluster information
    nclust = 0
    clusters = [] 
    centroids = [] # list to hold the centroid vector for each cluster

def euclidean_distance (v1: list, v2: list):
    res = 0
    for i in range(len(v1)):
        res += pow(v1[i] - v2[i], 2)
    return math.sqrt(res)

def initKmeans(data: dict, kmeansObject: KMeans, nclusts: int):
    kmeansObject.ndata = len(data)
    kmeansObject.dim = len(list(data.values())[0])
    for k, v in data.items():
        kmeansObject.features.append(v)
        kmeansObject.labels.append(k)
    kmeansObject.nclust = nclusts
    for i in range(nclusts):
        kmeansObject.clusters.append([])
def init_centroids(k: KMeans):
    '''
    farthest-first traversal method to initialize centroids
    Chooses points who are the farthest from the existing centroid
    '''
    # we start with the first data point we have as an arbituary centroid
    k.centroids.append(k.features[0]) 
    while (len(k.centroids) < k.nclust):
        # for each centroid calculate the distance between them and current centroids
        min_distances = []
        for feature in k.features:
            # calculating the distance between each feature and the centroids
            distances = [euclidean_distance(feature, centroid) for centroid in k.centroids]
            # we want to find the point which is the closest to the all the existing centroids
            min_dist = min(distances)
            min_distances.append(min_dist)
        # this is the point who is furthest away from our existing centroids
        max_dist = max(min_distances)
        # max_idx index for the feature that is the farthest away from our starting point
        max_idx = min_distances.index(max_dist) # recall that the min_distances have the same index as our original features list 
        k.centroids.append(k.features[max_idx])
def assignPoints(point: tuple[str, list[float]], centroids: list[float]):
    '''Assigns points to clusters based on how close they are to a centroid''' 
    new_clusters = [[] for _ in range(k.nclust)]
    label = point[0]
    feature = point[1]
    # find the closest centroid
    distances = []
    for centroid in centroids:
        distances.append(euclidean_distance(feature, centroid)) # finding the distance between the point and a centroid
    closest_idx = distances.index(min(distances)) # idx of the closest centroid
    new_clusters[closest_idx].append(label) # hold the id for whatever feature we decide to put in this cluster
    return new_clusters
def updateCentroids(k: KMeans):
    '''Updates the centroid by taking an average of all the points in a cluster'''
    new_centroids = []
    dim = k.dim
    for cluster in k.clusters:
        cur_centroid = []
        sum_vec = [0 for _ in range(dim)] # initializes sum vector 
        for feature_lbl in cluster:
            feature_idx = k.labels.index(feature_lbl)
            for i in range(dim): # for the entire feature
                sum_vec[i] += k.features[feature_idx][i]
        clust_len = len(cluster)
        if clust_len != 0:
            for v in range(len(sum_vec)):
                cur_centroid.append(sum_vec[v] / clust_len) # setting the centroid
            new_centroids.append(cur_centroid)
    return new_centroids    

def runKMeans(data_dict: dict, data, kmeansObj: KMeans, nclust: int, sc) -> None:
    '''
    @para: data_dict is a dictionary of all the features with the keys being the label
    @para: kmeansObj is the object which we manipulate to do the kmeans
    @para: nclust is the number of clusters 
    data is a pyspark rdd ("label", [features])
    '''
    # read in data to my kmeans structure
    initKmeans(data_dict, kmeansObj, nclust)
    # initialize centroids for my data 
    init_centroids(kmeansObj)
    sc.broadcast(kmeansObj.centroids)
    # keep iterating until our cluster centers stop moving or until max iter is hit
    for idx in range(MAX_ITER):
        assignPoints(kmeansObj)
        old_centroids = kmeansObj.centroids
        new_centroids = updateCentroids(kmeansObj)
        # check if Kmeans converges
        diff = []
        for i in range(len(new_centroids)):
            diff.append(euclidean_distance(old_centroids[i], new_centroids[i]))

        if sum(diff) == 0: # if we converge 
            break
        else: # we keep finding new centroid
            kmeansObj.centroids = new_centroids
