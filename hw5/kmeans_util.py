import math

MAX_ITER = 1000

def euclidean_distance (v1: list, v2: list):
    res = 0
    for i in range(len(v1)):
        res += pow(v1[i] - v2[i], 2)
    return math.sqrt(res)

def initCentroids(features: list[list[float]], num_clust: int):
    new_centroids = [features[0]]

    while len(new_centroids) < num_clust:
        max_dist = -1
        max_idx = -1
        for idx, feature in enumerate(features):
            min_dist = min(euclidean_distance(feature, centroid) for centroid in new_centroids)
            
            if min_dist > max_dist:
                max_dist = min_dist
                max_idx = idx

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

def updateCentroids(clusters: list[list[str]], cluster_lengths: list[int], to_feature: dict, dim, sc):
    import operator
    new_centroids = sc.parallelize(clusters).zipWithIndex().map(lambda x: (x[1], x[0])) \
        .flatMap(lambda x: [(x[0], label) for label in x[1]]) \
        .map(lambda x: (x[0], to_feature[x[1]])) \
        .flatMap(lambda x: [((x[0], i), value) for i, value in enumerate(x[1])]) \
        .reduceByKey(operator.add) \
        .map(lambda x: (x[0], x[1]/cluster_lengths[x[0][0]])) \
        .map(lambda x: (x[0][0], (x[0][1], x[1]))) \
        .groupByKey() \
        .mapValues(list) \
        .map(lambda x: createCentroid(x, dim)) \
        .sortBy(lambda x: x[0]) \
        .map(lambda x: x[1]) \
        .collect()
    return new_centroids

def findOutliers(clusters: list[list[float]]):
    inliers = []
    outliers = []
    outlier_threshold = 10
    for c in clusters:
        if len(c) <= outlier_threshold:
            outliers.append(c)
        else: # we are an inlier cluster 
            inliers.append(c)

    return inliers, outliers

def combine_cluster_lists(list1, list2):
    # Zip the elements of the two input lists and concatenate them
    concatenated_list = [a + b for a, b in zip(list1, list2)]
    # Filter out any empty elements from the concatenated list
    filtered_list = [list(filter(None, sublist)) for sublist in concatenated_list]
    return filtered_list

def runKmeans(data_sample, data_sample_dict, features, dim, n_cluster: int, sc):
    clusters = []
    centroids = sc.broadcast(initCentroids(features, n_cluster)) # we need to initialize our first run of kmeans to be some multiple of the number of clusters to avoid not converging because of outliers
    current_iter = 0
    prev_cluster = None
    old_lengths = None
    while (current_iter < MAX_ITER):
        clusters = data_sample.map(lambda x: assignPoints(x, centroids)) \
            .reduce(lambda x, y: combine_cluster_lists(x,y))
        cluster_lengths = [len(cluster) for cluster in clusters] # stores how many features are in each cluster used for finding the centroid later
        # print(f"iter: {current_iter} cluster: {cluster_lengths}") # DEBUG
        # old_centroids = centroids
        # new_centroids = updateCentroids(clusters, features, labels, dim)
        new_centroids = updateCentroids(clusters, cluster_lengths, data_sample_dict, dim, sc)
        # Compare the current clusters with the previous clusters
        if old_lengths == cluster_lengths or prev_cluster is not None and clusters == prev_clusters:
            break
        else:
            prev_clusters = clusters  # Update prev_clusters with the current clusters
            centroids = sc.broadcast(new_centroids)
            old_lengths = cluster_lengths
            # a = [len(c) for c in clusters] # DEBUG
            # print(a)
        # print(current_iter)
        current_iter += 1  
    return clusters, centroids.value # note that centroids was a broadcasted object