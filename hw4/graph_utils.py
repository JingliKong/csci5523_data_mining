import math
import queue 
import copy
from decimal import Decimal, getcontext
# set the precision you want
getcontext().prec = 28

class TreeNode:
    def __init__(self, name, height):
        self.name = name
        self.level = height #FIXME maybe dont need
        self.score = 1.0
        self.parent = set() # set of TreeNodes representing the parents
        self.children = set() # set of TreeNodes representing the nodes that are the children
    def __repr__(self):
        # return f"[name: {self.name}, level: {self.level}, parent: {self.parent}, children: {self.children}]"
        # return f"[name: {self.name}, parent: {self.parent}]"
        parents = []
        children = []
        score = self.score
        if len(self.children) != 0:
            children = [p.name for p in self.children]
        if len(self.parent) != 0:
            parents = [p.name for p in self.parent]
        
        return f"node:{self.name}, children: {children}, parents: {parents}, level: {self.level}, score: {score}"


def createGraph(nodes, edges):
    '''
    edges: [(0, 1), (0, 3), (0, 5), (0, 10), (0, 14), (0, 16), (0, 19), (0, 20), (0, 23), (0, 28)]
    nodes: [0, 1, 3, 5, 10, 14, 16, 19, 20, 23]
    '''
    graph = {node: [] for node in nodes}
    for node in nodes:
        for edge in edges:
            if node in edge:
                other_node = edge[0] if edge[1] == node else edge[1]
                if other_node not in graph[node]:
                    graph[node].append(other_node)
                
    return graph
# testing
# test_nodes = ['A', 'B', 'C', 'D', 'E', 'F', 'G']
# test_edges = [('A', 'B'), ('A', 'C'), ('C', 'B'), ('D', 'B'), ('E', 'D'), ('F', 'D'), ('G', 'D'), ('F', 'E'), ('F', 'G')]
# print(createGraph(test_nodes, test_edges))

# source: https://www.tutorialspoint.com/python_data_structure/python_graphs.htm
# I am using how this article describes graphs in python to represent the graphs for my task2
class MyGraph:
    # Constructor
    def __init__(self, gdict=None, directed=False):
        # we have a directed graph
        self.directed = directed 
        if gdict == None: 
            self.gdict = []

        self.gdict = copy.deepcopy(gdict) # nodes and their neighbors "A" : ["B","C"],
        # number of nodes we have 
        self.num_nodes = len(self.gdict)
        # the nodes in our graph
        self.nodes = list(self.gdict.keys())
        # hold a dictionary of how many edges each node has
        self.nodes_edges_dict = self.nodesEdgesDict()
        # initializing our edges to a list
        self.edges = self.findEdges()
        self.edgeDict = self.createEdgeDict()
    
    # gets the number of nodes aka vertices in the 
    
    def getVertices(self):
        return list(self.gdict.keys())
    
    def findEdges(self):
        edges = []
        for vrtx in self.gdict:
            for nxtvrtx in self.gdict[vrtx]:
                candidate_edge = frozenset((vrtx, nxtvrtx)) # now we dont have duplicate edges
                # candidate_edge = (vrtx, nxtvrtx)
                if candidate_edge not in edges:
                    edges.append(candidate_edge)
        return edges
    def removeEdge(self, edge: tuple):
        '''
        edge: frozenset({'D', 'B'})
        removes an edge from the graph and updates all the nodes and edge dictionaries accordingly
        '''
        edge = tuple(edge)
        self.gdict[edge[0]].remove(edge[1])
        self.gdict[edge[1]].remove(edge[0])
        self.nodes = list(self.gdict.keys())
        self.edges = self.findEdges()
        self.edgeDict = self.createEdgeDict()
        return self
    def createEdgeDict(self):
        return {e: 0 for e in self.findEdges()}
    def nodesEdgesDict(self): # used for k_i and k_j during community detection
        output_dict = {}
        for k,v in self.gdict.items():
            output_dict[k] = len(v)
        return output_dict
    def createTree(self, start):
        '''
        Build a tree using TreeNode using bfs
        start: the starting node for our bfs 
        return: a list of all the TreeNodes created from bfs 
        '''
        nodes = []
        bfs_results = []
        visited_nodes: list[str] = []  # names of the nodes that we have visited
        # first initialize all the TreeNodes we'll need for this tree
        for n in self.nodes:
            nodes.append(TreeNode(n, math.inf))
        # we have to find our starting node
        root = None
        for n in nodes:
            if n.name == start:
                root = n
                n.level = 0
                break
        # add the root to the visited_nodes list
        visited_nodes.append(root.name)
        # create a queue starting at the starting node which is the root
        assert (root != None)
        myQueue = queue.Queue()
        myQueue.put(root)
        # initialize the shortest path counts
        shortest_paths_count = {node.name: 0 for node in nodes}
        shortest_paths_count[root.name] = 1
        # iterate until the queue is empty
        while (myQueue.qsize() != 0):  # TODO add max iter
            current_node = myQueue.get()
            neighbors = self.gdict[current_node.name]  # list of nodes that are neighbors
            # look through allocated nodes and see which ones are our neighbors
            for n in neighbors:
                for node in nodes:
                    if node.name == n and node.level > current_node.level:
                        node.level = current_node.level + 1
                        node.parent.add(current_node)  # setting the parent as a ref to the current node
                        current_node.children.add(node)
                        if (node.name not in visited_nodes):
                            myQueue.put(node)
                            visited_nodes.append(node.name)
                        # update the shortest path count
                        shortest_paths_count[node.name] += shortest_paths_count[current_node.name]
            bfs_results.append(current_node)
        # assign scores to each node based on the shortest path count
        for node in nodes:
            node.score = shortest_paths_count[node.name] / shortest_paths_count[start]
        return bfs_results

      
    def bfs(self, start):
        # returns strings of all the nodes visited following a bfs                  
        myQueue = queue.Queue()
        myQueue.put(start)
        visited = []
        while (myQueue.qsize() != 0):
            node = myQueue.get()
            if node not in visited:
                visited.append(node)
            for neighbor in self.gdict[node]:
                if neighbor not in visited:
                    visited.append(neighbor) 
                    myQueue.put(neighbor)
        return visited  
def calcWeightsForGN(graph: MyGraph, tree: list[TreeNode]) -> dict[frozenset]:
    '''assigns the node weights and edge weights for the Girvan-Newman alg'''
    edge_betweeness = {edge: 0 for edge in graph.edges}
    # we need to start at highest level and recurse from max height back to 0
    starting_level = tree[len(tree)-1].level # we start from the leaf nodes onward to calculate betweeness
    levels_of_tree = [] # a list to hold the number of nodes per level index 3 contains how many nodes at height 3
    while (starting_level >= 0):
        per_level = []
        for node in tree:
            if node.level == starting_level:
                per_level.append(node)
        levels_of_tree.append(per_level)
        starting_level -= 1

    # first covering the case for the leaves of the tree
    for leaf in levels_of_tree[0]:
        credit = 1  
        for p in leaf.parent:
            ratio = p.score / leaf.score 
            local_credit = credit * ratio
            edge = frozenset((leaf.name, p.name))
            edge_betweeness[edge] += local_credit
    # at this point we finished adding the weights for all the leaves 
    # computing weights for all other nodes so we have to include the weights of edges coming in
    for level in range(1, len(levels_of_tree), 1):
        for node in levels_of_tree[level]:
            credit = 1 # default credit
            for child in node.children:
                edge_coming_in = frozenset((child.name, node.name))
                credit += edge_betweeness[edge_coming_in] 
            for p in node.parent:
                ratio = p.score / node.score
                local_credit = credit * ratio
                edge = frozenset((node.name, p.name))
                edge_betweeness[edge] += local_credit

    return edge_betweeness

def find_communities(graph: MyGraph):
    communities = []
    nodes_visited = set()
    for node in graph.nodes:
        if node not in nodes_visited:
            community = set(graph.bfs(node))
            communities.append(community)
            nodes_visited = nodes_visited.union(community)
    return communities    


def findModularity(starting_graph: dict, current_graph: MyGraph, m: int):
    '''m is the number of edges in the orginal graph'''
    from itertools import combinations
    communities = find_communities(current_graph)
    total = 0 
    # m = len(starting_graph.edges)
    for community in communities:
        pairs = combinations(community, 2)
        for p in pairs:
            node_i = p[0]
            node_j = p[1]
            k_i = current_graph.nodes_edges_dict[node_i]
            k_j = current_graph.nodes_edges_dict[node_j]
            A_i_j = 1 if node_j in starting_graph[node_i] else 0
            total += A_i_j - (k_i * k_j / (2*m))
    return (communities, total / (2*m)) # communities, mod score
# testing finding communities 
# graph_elements = { 
#    "A" : ["B","C"],
#    "B" : ["A", "C", "D"],
#    "C" : ["A", "B"],
#    "D" : ["B", "E", "F", "G"],
#    "E" : ["D", "F"],
#    "F" : ["D", "E", "G"],
#    "G" : ["D", "F"]
# }

 
# starting_graph = MyGraph(graph_elements)
# trees = []
# for k, v in graph_elements.items():
#     trees.append(starting_graph.createTree(k))
#     all_betweenesses: list[dict] = []
# for tree in trees:
#     all_betweenesses.append(calcWeightsForGN(starting_graph, tree))
#     sum_betweeness = starting_graph.createEdgeDict()
# for b in all_betweenesses: 
#     for k, v in b.items():
#         sum_betweeness[k] += v
# sum_betweeness = {k: v/2 for k, v in sum_betweeness.items()} 
# largest_betweenness = max(sum_betweeness, key=sum_betweeness.get)
# current_graph = MyGraph(starting_graph.gdict).removeEdge(largest_betweenness)

# modularity = findModularity(starting_graph, current_graph)
# best_modularity = modularity # (community, modularity score)

# next_graph = None
# for i in range(len(starting_graph.edges) - 1):
#     trees = []
#     for k, v in graph_elements.items():
#         trees.append(current_graph.createTree(k))
#         all_betweenesses: list[dict] = []
#     for tree in trees:
#         all_betweenesses.append(calcWeightsForGN(current_graph, tree))
#         sum_betweeness = current_graph.createEdgeDict()
#     for b in all_betweenesses: 
#         for k, v in b.items():
#             sum_betweeness[k] += v
#     sum_betweeness = {k: v/2 for k, v in sum_betweeness.items()} 
#     largest_betweenness = max(sum_betweeness, key=sum_betweeness.get)
#     next_graph = MyGraph(current_graph.gdict).removeEdge(largest_betweenness)
#     modularity = findModularity(starting_graph, current_graph)
#     if (modularity[1] > best_modularity[1]):
#         best_modularity = modularity
# print()
# TESTING

# graph_elements = { 
#    "a" : ["b","c"],
#    "b" : ["a", "d"],
#    "c" : ["a", "d"],
#    "d" : ["e"],
#    "e" : ["d"]
# }
# g = MyGraph(graph_elements)
# print(g.findEdges()) # [{'b', 'a'}, {'a', 'c'}, {'b', 'd'}, {'d', 'c'}, {'e', 'd'}] sets or we can also use tuples if needbe 
# print(g.edges()) # [('a', 'b'), ('a', 'c'), ('b', 'a'), ('b', 'd'), ('c', 'a'), ('c', 'd'), ('d', 'e'), ('e', 'd')]

# edge_dict = g.edgeDict
# print("keys:")
# for k in edge_dict.keys():
#     print(k)

# testing Girvan newman alg

# graph_elements = { 
#    "A" : ["B","C"],
#    "B" : ["A", "C", "D"],
#    "C" : ["A", "B"],
#    "D" : ["B", "E", "F", "G"],
#    "E" : ["D", "F"],
#    "F" : ["D", "E", "G"],
#    "G" : ["D", "F"]
# }

# create a graph using graph elements 
# g = MyGraph (graph_elements)
# print(g.nodes)
# print([tuple(item) for item in g.edges])
# !!!!!!!!EACH PROC NEEDS ITS OWN GRAPH MY DUDE

# create a tree from my graph from a particular node
# tree = g.createTree("E")
# for t in tree:
#     print(t)
# for the tree calculate all the edges and weights for nodes
# everything works we guchi
# betweeness = calcWeightsForGN(g, tree)  # {frozenset({'A', 'B'}): 1.0, frozenset({'A', 'C'}): 0, frozenset({'B', 'C'}): 1.0, frozenset({'B', 'D'}): 3.0, frozenset({'G', 'D'}): 0.5, frozenset({'E', 'D'}): 4.5, frozenset({'E', 'F'}): 1.5, frozenset({'F', 'D'}): 0, frozenset({'F', 'G'}): 0.5}

# testing if we can calculate the betweeness for the entire graph
# nodes that we bfs we will eventually use them to calculate betweeness 

# graph_elements = { 
#    "A" : ["B","C"],
#    "B" : ["A", "C", "D"],
#    "C" : ["A", "B"],
#    "D" : ["B", "E", "F", "G"],
#    "E" : ["D", "F"],
#    "F" : ["D", "E", "G"],
#    "G" : ["D", "F"]
# }
# g = MyGraph (graph_elements)
# trees = []
# for k, v in graph_elements.items():
#     trees.append(g.createTree(k))

# all_betweenesses: list[dict] = []
# for tree in trees:
#     all_betweenesses.append(calcWeightsForGN(g, tree))

# sum_betweeness = g.createEdgeDict()
# for b in all_betweenesses: 
#     for k, v in b.items():
#         sum_betweeness[k] += v
# sum_betweeness = {k: v/2 for k, v in sum_betweeness.items()} 
# print(sum_betweeness)

# yo this works lets goo