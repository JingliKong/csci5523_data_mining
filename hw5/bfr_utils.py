from kmeans import *
import math
class Discard_Set:
    def __init__(self, data_points: list[list[float]], data_indices: list[str]) -> None:
        self.feature_dim = len(data_points[0]) # the length of a feature vec
        self.N = len(data_points)
        self.SUM = [sum(x) for x in zip(*data_points)]
        self.SUMSQ = [sum(x_i ** 2 for x_i in x) for x in zip(*data_points)]
        self.data_indices = data_indices.copy()

        self.variance = self.calc_variance()
        self.centroid = [i / self.N for i in self.SUM]
    def calc_variance(self) -> float:
        variance = []
        for i in range(self.feature_dim):
            variance.append((self.SUMSQ[i] / self.N) - (self.SUM[i] / self.N)**2)
        return variance
    def calc_centroid(self) -> list[float]:
        for i in range(len(self.centroid)):
            self.centroid[i] = self.centroid[i] / self.N
   
    def merge(self, other):
        self.N += other.N
        self.SUM = [a + b for a, b in zip(self.SUM, other.SUM)]
        self.SUMSQ = [a + b for a, b in zip(self.SUMSQ, other.SUMSQ)]
        self.data_indices += other.data_indices

    def add_point(self, data_point: list[float], data_index: int):
        self.N += 1
        self.SUM = [a + b for a, b in zip(self.SUM, data_point)]
        self.SUMSQ = [a + b * b for a, b in zip(self.SUMSQ, data_point)]
        self.data_indices.append(data_index)
    
    def __repr__(self):
        return f"centroid: {str(self.get_centroid())}, variance: {str(self.calc_variance())}"
    
def BuildDiscardSets(data_points: dict, cluster_data: list):
    return [Discard_Set([data_points[x] for x in cluster], cluster) for cluster in cluster_data]
# def BuildDiscardSets(data_points: dict, cluster_data: list):
#     discard_sets = []
    
#     cluster_data = [data_points[x] for x in cluster_data]

#     for cluster in cluster_data:
#         data_in_cluster = [data_points[x] for x in cluster]
#         discard_set = Discard_Set(data_in_cluster, cluster)
#         discard_sets.append(discard_set)

#     return discard_sets



class Compression_Set(Discard_Set): # The compression set and Discard_Set have the same structure
    def __init__(self, centroid: list[float], points: list[list[float]], labels: list[int]) -> None:
        super().__init__(centroid, points, labels)

class Retained_Set:  # retained set
    def __init__(self, data_points, rs_data_indices):
        self.data_points = {index: data_points[index] for index in rs_data_indices}

    def add_points(self, adding_data_points: dict):
        self.data_points.update(adding_data_points)

        
def mahalanobis_distance(data_point: list[float], centroid: list[float], variance: float) -> float:
    vector = []
    for i in range(len(data_point)):
        x_i = data_point[i]
        c_i = centroid[i]
        sigma_i = variance[i]
        vector.append(((x_i - c_i) / math.sqrt(sigma_i)) ** 2)
    return math.sqrt(sum(vector))

def assignToSet(points: dict, list_of_sets: list):
    # if not DS_CS_list:
    #     return data_points
    '''Attempts to assign point to one of my sets (compressed or retained or discard)'''
    alpha = 2 
    assigned_data_indices = set()
    for idx, data_point in points.items():
        min_distance = float('inf')
        closest_set = None
        
        for _set in list_of_sets:
            # for each data point 
            distance = mahalanobis_distance(data_point, _set.centroid, _set.variance)
            
            if distance < min_distance:
                min_distance = distance
                closest_set = _set
        
        if min_distance > math.sqrt(len(data_point)) * alpha:
            continue
        
        closest_set.add_point(data_point, idx)
        assigned_data_indices.add(idx)
        
    return {k: v for k, v in points.items() if k not in assigned_data_indices}

def mergeSets(list_of_sets: list):
    while len(list_of_sets) > 1:
        num_dimension = len(list_of_sets[0].SUM)
        min_distance = float('inf')
        min_i = -1
        min_j = -1
        # for each pair of sets 
        for i in range(len(list_of_sets)):
            for j in range(i + 1, len(list_of_sets)):
                # updating centroid information
                c1 = list_of_sets[i].calc_centroid()
                c1 = list_of_sets[i].centroid
                c2 = list_of_sets[j].calc_centroid()
                c2 = list_of_sets[j].centroid
                v1 = list_of_sets[j].calc_variance()
                v2 = list_of_sets[i].calc_variance()
                distance = max(mahalanobis_distance(c1, list_of_sets[j], v1),
                               mahalanobis_distance(c2, list_of_sets[i], v2))
                if distance < min_distance:
                    min_distance = distance
                    min_i = i
                    min_j = j
        if min_distance > math.sqrt(num_dimension) * 2:
            break
        list_of_sets[min_i].merge(list_of_sets[min_j])
        list_of_sets.pop(min_j)

    return list_of_sets

def getNumPoints(_sets: list):
    return sum(x.N for x in _sets)
