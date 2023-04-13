class TreeNode:
    def __init__(self, name, height):
        self.name = name
        self.level = height #FIXME maybe dont need
        self.score = 1
        self.parent = set() # set of TreeNodes representing the parents
        self.children = set() # set of TreeNodes representing the nodes that are the children
    def __repr__(self):
        return f"[name: {self.name}, level: {self.level}, parent: {self.parent}, children: {self.children}]"
        # return f"[name: {self.name}, parent: {self.parent}]"

# source: https://www.tutorialspoint.com/python_data_structure/python_graphs.htm
# I am using how this article describes graphs in python to represent the graphs for my task2
class MyGraph:
    # Constructor
    def __init__(self, gdict=None, directed=False):
        # we have a directed graph
        self.directed = directed 
        if gdict == None: 
            self.gdict = []

        self.gdict = gdict
        # number of nodes we have 
        self.num_nodes = len(self.gdict)
        # the nodes in our graph
        self.nodes = list(self.gdict.keys())
        # initializing our edges to a list
        self.edges = self.findEdges()
        self.edgeDict = {e: 0 for e in self.findEdges()}
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
    def createEdgeDict(self):
        self.edges = {e: 0 for e in self.findEdges()}
    def createTree (self, start):
        '''
        Build a tree using TreeNode using bfs
        start: the starting node for our bfs 
        return: a list of all the TreeNodes created from bfs 
        '''
        visited = []
        name_of_visited_nodes = []
        queue = [TreeNode(start, height=0)]
        created_nodes: list[str] = [] 
        # loop until we have visited all the nodes
        while len(queue) != 0: # FIXME: Include a maxiter incase we have a disconnected graph
            # the node we are exploring
            node = queue.pop(0)
            # getting the name
            name = node.name
            level = node.level
            # checking if we have visited this node before
            if node.name not in name_of_visited_nodes:
                # adding the current node to a list maintaining all the nodes we have visited
                name_of_visited_nodes.append(name)
                # getting all neighbors and adding them to queue
                neighbors = []
                for n in self.gdict[node.name]:
                    if (n not in name_of_visited_nodes):
                        neighbors.append(n)
                # for each child
                for neighbor in neighbors:
                    # we add all nodes that this node connect to as children for our bfs tree
                    node.children.add(neighbor) 
                    if neighbor not in created_nodes:
                        # we allocate the current neighbor and set its level 
                        neighbor_node = TreeNode(neighbor, level+1)
                        # each of these newly allocated TreeNodes have the current explored node as their parent
                        neighbor_node.parent.add(node)
                        # appending the current node to the queue
                        queue.append(neighbor_node)
                        # keep track of variables allocated
                        created_nodes.append(neighbor)
                    else: # node already is in queue so we have to edit it's parent to append the current node
                        for i in range(len(queue)):
                            if queue[i].name == neighbor:
                                queue[i].parent.add(node)
                
                # at this point we have to add the node to our visited list 
                visited.append(node) # node is a TreeNode
        # sets all the leaf node's children to be nothing because they are leaves
        total_height = visited[-1].level
        for n in visited:
            node_parents = n.parent.copy()  # create a copy of the set
            for p in node_parents:
                if p.level == n.level:
                    n.parent.remove(p)  # modify the original set

            if (n.level == total_height):
                n.children = set()
        # removing extra parents just because a node is a neighbor doesn't mean that its a parent

        return visited 
def calcWeightsForGN(graph: MyGraph, queue: list[TreeNode]) -> dict[frozenset]:
    '''assigns the node weights and edge weights for the Girvan-Newman alg'''
    edge_betweeness = {edge: 0 for edge in graph.edges}
    # to store current nodes we are exploring we start exploring one of the leaf nodes 

    # calculating edge weights and the weights of all nodes
    # recall that the queue starts off with the leaf nodes
    while(len(queue) > 0): 
        # recall: A DAG edge e entering node Z from the level above is given a share of the
        # credit of Z proportional to the fraction of shortest paths from the root to Z that go through pg 12 in chap 10 mmds
        current_node = queue.pop()
        # a literally reference to parent node addresses
        parents = []
        for p in current_node.parent: # p is a reference to another node which is a parent
            parents.append(p)
        if len(parents) != 0:
            credit_share = current_node.score / len(parents)
        else:
            credit_share = 0
        # holds all the edges from current node to parent(s)
        for p in parents: 
            # creates edges from our current node so we know which edges to give credit to
            current_edge = frozenset((current_node.name, p.name))
            # adding our credit to the edge
            edge_betweeness[current_edge] += credit_share
            # we also increase our parent node's score based on the credit we give them
            p.score += credit_share

    return edge_betweeness


    
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

graph_elements = { 
   "A" : ["B","C"],
   "B" : ["A", "D", "C"],
   "C" : ["A", "B"],
   "D" : ["E", "F", "G"],
   "E" : ["D", "F"],
   "F" : ["D", "E", "G"],
   "G" : ["D", "F"]
}

# create a graph using graph elements 
g = MyGraph (graph_elements)

# !!!!!!!!EACH PROC NEEDS ITS OWN GRAPH MY DUDE

# create a tree from my graph from a particular node
tree = g.createTree("E")
# for t in tree:
#     print(t)
# for the tree calculate all the edges and weights for nodes
# everything works we guchi
betweeness = calcWeightsForGN(g, tree)  # {frozenset({'A', 'B'}): 1.0, frozenset({'A', 'C'}): 0, frozenset({'B', 'C'}): 1.0, frozenset({'B', 'D'}): 3.0, frozenset({'G', 'D'}): 0.5, frozenset({'E', 'D'}): 4.5, frozenset({'E', 'F'}): 1.5, frozenset({'F', 'D'}): 0, frozenset({'F', 'G'}): 0.5}

# testing if we can calculate the betweeness for the entire graph
# nodes that we bfs we will eventually use them to calculate betweeness 
nodes = []
for k, v in graph_elements.items():
    nodes.append(g.createTree(k))

all_betweenesses: list[dict] = []
for n in nodes:
    all_betweenesses.append(calcWeightsForGN(g, n))

sum_betweeness = {}
# everyone has the same keys so we just add all their values up
for d in all_betweenesses:
    for k, v in d.items():
        if k not in sum_betweeness:
            sum_betweeness[k] = v
        else:
            sum_betweeness[k] += v 
 

print()