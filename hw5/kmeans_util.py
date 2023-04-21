import math

MAX_ITER = 1000

def euclidean_distance (v1: list, v2: list):
    res = 0
    for i in range(len(v1)):
        res += pow(v1[i] - v2[i], 2)
    return math.sqrt(res)

def initCentroids(features: list[list[float]], num_clust: int):
    # assigning initial centroids
    new_centroids = []
    new_centroids.append(features[0])
    while (len(new_centroids) < num_clust):
        # for each centroid calculate the distance between them and current centroids
        min_distances = []
        for feature in features:
            # calculating the distance between each feature and the centroids
            distances = [euclidean_distance(feature, centroid) for centroid in new_centroids]
            
            # we want to find the point which is the closest to the all the existing centroids
            min_dist = min(distances)
            min_distances.append(min_dist)
        # this is the point who is furthest away from our existing centroids
        max_dist = max(min_distances) 
        max_idx = min_distances.index(max_dist) # recall that the min_distances have the same index as our original features list 
        new_centroids.append(features[max_idx])
    return new_centroids
def assignPoints(point: tuple[str, list[float]], centroids: list[float]):
    '''Assigns points to clusters based on how close they are to a centroid''' 
    centroids = centroids.value
    new_clusters = [[] for _ in range(len(centroids))]
    label = point[0]
    feature = point[1]
    # find the closest centroid
    distances = []
    for centroid in centroids:
        distances.append(euclidean_distance(feature, centroid)) # finding the distance between the point and a centroid
    closest_idx = distances.index(min(distances)) # idx of the closest centroid
    new_clusters[closest_idx].append(label) # hold the id for whatever feature we decide to put in this cluster
    return new_clusters

def updateCentroids(clusters, labels, features, dim):
    '''Updates the centroid by taking an average of all the points in a cluster'''
    new_centroids = []
    for cluster in clusters:
        cur_centroid = []
        sum_vec = [0 for _ in range(dim)] # initializes sum vector 
        for feature_lbl in cluster:
            feature_idx = labels.index(feature_lbl)
            for i in range(dim): # for the entire feature
                sum_vec[i] += features[feature_idx][i]
        clust_len = len(cluster)
        if clust_len != 0:
            for v in range(len(sum_vec)):
                cur_centroid.append(sum_vec[v] / clust_len) # setting the centroid
            new_centroids.append(cur_centroid)
    return new_centroids    

def createCentroid(centroid_data, centroid_dim):
    cluster_num = centroid_data[0] # literlly the cluster for which this centroid belongs to
    centroid = [0] * centroid_dim
    for d in centroid_data[1]:
        index = d[0]
        value = d[1]
        centroid[index] = value
    return (cluster_num, centroid)
    