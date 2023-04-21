from kmeans_util import *
import math

class Discard_Set: # discarded set
    def __init__(self, centroid: list[float], points: list[list[float]], labels: list[int]) -> None:
        self.feature_dim = len(points[0]) # the length of a feature vec
        self.N: int = len(points)
        self.SUM: list[float] = []
        self.SUMSQ: list[float] = []
        self.labels: list[int] = labels # holds the labels for the data points which are in the discard set
        self.centroid: list[float] = centroid  # add centroid attribute

        self.SUM = [0.0] * self.feature_dim
        self.SUMSQ = [0.0] * self.feature_dim
        self.centroid = centroid

        for p in points:
            self.SUM = [a + b for a, b in zip(self.SUM, p)]
            self.SUMSQ = [a + b**2 for a, b in zip(self.SUMSQ, p)]

        self.variance =self.calc_variance()

    def calc_variance(self) -> float:
        variance = []
        for i in range(self.feature_dim):
            variance.append((self.SUMSQ[i] / self.N) - (self.SUM[i] / self.N)**2)
        return variance
    def calc_centroid(self) -> list[float]:
        for i in range(len(self.centroid)):
            self.centroid[i] = self.centroid[i] / self.N
    def addPoint(self, label: str, feature: list[float]):
        '''When we add a point to the discard set we update the stats'''
        self.SUM = [a + b for a, b in zip(self.SUM, feature)]
        self.SUMSQ = [a + b**2 for a, b in zip(self.SUMSQ, feature)]
        self.labels.append(label)

class Compression_Set(Discard_Set): # The compression set and Discard_Set have the same structure
    def __init__(self, centroid: list[float], points: list[list[float]], labels: list[int]) -> None:
        super().__init__(centroid, points, labels)

class RetainedSet:
    def __init__(self) -> None:
        self.N = 0
        self.features = []
        self.labels = []
    def add_point(self, data_point: tuple):
        self.N += 1
        self.labels.append(data_point[0])
        self.features.append(data_point[1])
        
def mahalanobis_distance(data_point: list[float], centroid: list[float], variance: float) -> float:
    vector = []
    for i in range(len(data_point)):
        x_i = data_point[i]
        c_i = centroid[i]
        sigma_i = variance
        vector.append(((x_i - c_i) / math.sqrt(sigma_i)) ** 2)
    return math.sqrt(sum(vector))

def findSet(clusters, centroids, CS, ): 
    
    return 

def createDSList(clusters, label_to_feature: dict, centroids):

    cluster_index = clusters[1]
    cluster_data = clusters[0] 
    cluster_features: list[list[float]] = [label_to_feature[label] for label in cluster_data]
    return Discard_Set(centroids[cluster_index], cluster_features, cluster_data)
