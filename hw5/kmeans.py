import math, random

MAX_ITER = 1000

def euclidean_distance(data1: list, data2: list):
    if len(data1) != len(data2):
        raise Exception("Two data points must have same dimension!")
    res = 0
    for i in range(len(data1)):
        res += pow(data1[i] - data2[i], 2)
    return math.sqrt(res)


class KMeans:
    def __init__(self, data_points: dict, num_cluster: int):

        self.num_clusters = num_cluster
        self.data_points = data_points
        
        self.features = list(data_points.values())
        # maps the features back to their original key value in the data_points dict
        self.label_to_feature = {i: data_index for i, data_index in enumerate(data_points)}
        self.num_data = len(self.features)
        self.dim = len(self.features[0])

        
        self.clusters = [[] for _ in range(self.num_clusters)]
        self.centroids = []  # centroid vector of each cluster
        # runs the clustering algorithm
        self.runKmeans()

    def runKmeans(self):
        # initialize centroids
        self.initCentroids()
        # optimization
        for _ in range(MAX_ITER):
            # update clusters
            self.clusters = self.updateClusters()
            # update centroids
            centroids_old = self.centroids
            self.centroids = self.updateCentroids()
            # check if converged
            if self.isConverged(centroids_old):
                break
    def initCentroids(self):
        # farthest first traversal algorithm
        self.centroids = [self.features[0]]
        min_distances = [euclidean_distance(data, self.centroids[0]) for data in self.features]
        for _ in range(1, self.num_clusters):
            # finding the index of the point that is the max distance from the current centroid
            farthest_point_idx = min_distances.index(max(min_distances))
            # finds the feature that is the farthest away 
            farthest_point = self.features[farthest_point_idx]
            # appending the point that is farthest 
            self.centroids.append(farthest_point) 
            # iterating through the rest of the points in the chunk
            for i, data in enumerate(self.features):
                # finding dist between current data point and the farthest one 
                new_distance = euclidean_distance(data, farthest_point)
                # updating minimum distance
                min_distances[i] = min(min_distances[i], new_distance)
    def updateClusters(self):
        clusters = [[] for _ in range(self.num_clusters)]
        for idx, data in enumerate(self.features):

            # Find the closest centroid index for the current data point
            distances = [euclidean_distance(data, centroid) for centroid in self.centroids]
            closest_idx = distances.index(min(distances))
            # Add the data point index to the corresponding cluster
            clusters[closest_idx].append(idx)

        return clusters

    def findOutliers(self, threshold):
        # above_clusters = [cluster in self.get_cluster_data() if len(cluster) > threshold]
        # below_clusters = [cluster in self.get_cluster_data() if len(cluster) > threshold]
        above_clusters, below_clusters = [], []
        for cluster in self.get_cluster_data():
            if len(cluster) > threshold:
                above_clusters.append(cluster)
            else:
                below_clusters.append(cluster)
        return above_clusters, below_clusters
    
    def updateCentroids(self):
        centroids = []
        for cluster in self.clusters:
            sum_vector = [0 for _ in range(self.dim)]
            for idx in cluster:
                sum_vector = [a + b for a, b in zip(sum_vector, self.features[idx])]
            if (len(cluster) != 0):
                centroids.append([i / len(cluster) for i in sum_vector])
            else: 
                 centroids.append([i for i in sum_vector]) # if there are no element in the cluster just keep the old centroid
        return centroids

    def isConverged(self, centroids_old):
        for i in range(self.num_clusters):
            if euclidean_distance(centroids_old[i], self.centroids[i]) > 1e-4:
                return False
        return True


    def findDataOutliers(self, threshold):
        if threshold == None:
            threshold = 10
        inliers = [self.label_to_feature[label] for cluster in self.clusters if len(cluster) > threshold for label in cluster]
        outliers = [self.label_to_feature[label] for cluster in self.clusters if len(cluster) <= threshold for label in cluster]

        return inliers, outliers

    def findClusterData(self):
        return [[self.label_to_feature[x] for x in cluster] for cluster in self.clusters]
    
    def get_cluster_data(self):
        return list(map(lambda x: list(map(lambda x: self.label_to_feature[x], x)), self.clusters))


    

    def _data_points_to_list(self):
        self.X = []
        self.index_map = dict()
        for i, (data_index, data_vector) in enumerate(self.data_points.items()):
            self.X.append(data_vector)
            self.index_map[i] = data_index
        self.num_data = len(self.X)
        self.dimension_data = len(self.X[0])





