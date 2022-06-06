"""
Project: HIV prevention
Version: 1.0
Date: 03-Jun-2022

red_change.py library include functions to solve the Maximum Influence Problem , the creation and manipulation of Social Graph the dataset.
"""
import networkx as nx
import numpy as np
import random
import sys

# computeDistance performs the euclidean distance from a node to the set of already discovered nodes
def computeDistance(node, V):
    m = sys.maxsize
    for j in V:
        n = np.array(node[1:])    #avoid the ID
        x = np.array(j[1:])
        m = min(m, np.sum((n - x)**2))
    return m

# KMeansPlusPlus function is the replica of the K-Means++ algorithm by David Arthur and Sergei Vassilvitskii
def kMeansPlusPlus(nodes, V, graph, E):
    C = nodes.difference(V)
    nodes = []
    distribuition = []
    normalizing = 0
    for j in C:
        nodes.append(j)
        distance = computeDistance(j, V)
        normalizing += distance
        distribuition.append(distance)
    np.divide(distribuition,normalizing)
    vertex = random.choices(nodes, distribuition)
    vertex = tuple(vertex[0])
    V.add(vertex)
    for edge in graph.edges(vertex):
        E.add(edge)
    return vertex, V, E

# networkSamplingPlus takes as input the graph and the queries to ask to random nodes in it,
# it create the final Graph including the more prominent peer leaders by the Clustering approach.
def networkSamplingPlus(graph, M):
    E = set()
    V = set()
    first = random.choice(tuple(graph.nodes))
    V.add(first)
    for edge in graph.edges(first):
        E.add(edge)
    second = random.choice(tuple(graph.neighbors(first)))
    V.add(second)
    for edge in graph.edges(second):
        E.add(edge)
    for i in range(int(M/2 - 1)):
        nodes = set()
        for node in graph.nodes:
            nodes.add(node)
        try:
            vertex, v, e = kMeansPlusPlus(nodes, V, graph, E)
        except IndexError as e:
            break
        V = v
        E = e
        nodes = set()
        for node in graph.neighbors(vertex):
            nodes.add(node)
        try:
            vertex, v, e = kMeansPlusPlus(nodes, V, graph, E)
        except IndexError as e:
            break
        V = v
        E = e
    G = nx.Graph()
    G.add_nodes_from(V)
    G.add_edges_from(E)
    return G

# createNetworks takes in input all the subset and dictionaries with the mapping of label for each value in each cathegory
# , it returns the list of social network built.
def createNetworks(starting_set, dictionaries):   
    networks = []
    for i in range(len(starting_set)):
        entries = (starting_set[i].collect())
        vertices = []
        for entry in entries:
            vertex = [entry[0]]
            for j in range(2, len(entry)-1):
                #age or center ID
                if (j == 3 or j == 11): 
                    if entry[j] == 'None':
                        vertex.append(0)
                    else:
                        vertex.append(int(entry[j]))
                    continue
                #living status
                if j == 10: 
                    continue
                value = entry[j]
                dictionary = dictionaries[j-2]
                to_append = int(dictionary[value])
                vertex.append(to_append)
            vertices.append(tuple(vertex))
        G = nx.Graph()
        for v in vertices:
            G.add_node(v)
        for j in vertices:
            for k in vertices:
                if (k > j):
                    exists = random.randint(0,1)
                    if exists > 0.5:
                        G.add_edge(j, k)
        networks.append(G) 
    return networks

# parameterSelection is the Robust Optimization part of the RedChange algorithm, it computes the 
# most preferable propagation probability values to be used for the selection of the peer leaders.
# It returns the value (propagation probability) found and the table of probabilities
def parameterSelection(T, K, graph):
    probabilities = [i/3 for i in range(3)]
    values = np.zeros((len(probabilities),len(probabilities)))
    for i in range(len(probabilities)):
        for j in range(len(probabilities)):
            estimated_value = 0
            for k in range(2):
                estimated_value += adaptiveGreedy(T, K, graph, i, j)
            estimated_value /= 2
            values[i][j] = estimated_value
    mins = np.zeros(len(probabilities))
    for r in range(len(probabilities)):
        row = values[r, :]
        stub = row.copy()
        for j in range(len(stub)):
            stub[j] /= values[j][j]
        np.append(mins, stub.min())    
    idx = mins.argmax()
    return probabilities[idx], values

# adaptiveGreedy is the greedy solution to finds a set which maximize the minimum expected coverage
# by considering the current error of the propagation probability it returns the list of the reached peers
def adaptiveGreedy(T, K, graph, estimated_prob, real_prob):
    temp_reached = []
    not_answering = []
    for i in range(T):
        A = []
        for j in range(K):
            vertex = selectVertex(A, graph, estimated_prob, temp_reached, not_answering, i)
            A.append(vertex)
        new_reached,_,not_answered = makeARun(graph, A, real_prob, temp_reached, i)
        for re in new_reached:
            temp_reached.append(re)
        for el in not_answered:
            not_answering.append(el)
    return len(temp_reached)

# selectVertex function :
# it returns the vertex which influenced the most in the Simulations of the experiment.
def selectVertex(A, graph, p, reached, not_answering, t):
    maxim = None
    m_rsize = 0
    nt = []
    for tuple in not_answering:
        nt.append(tuple[0])
    papables = set(graph.nodes).difference(set(A))
    nt = set(nt)
    papables = list(papables.difference(nt))
    for vertex in papables:
        rsize = 0
        B = A.copy()
        B.append(vertex)
        for i in range(2):
            r,_,_ = makeARun(graph, B, p, reached, t)
            rsize += len(r)
        rsize /= 2
        if rsize > m_rsize:
            m_rsize = rsize
            maxim = vertex
    return maxim

# makeARun has an attempt to evaluate the influence effectiveness of the current set,
# it returns the nodes reached , the list of observation done and the list of absent.
def makeARun(graph, leaders, p, reached, t):
    not_answered = []
    new_reached = []
    observation = []
    for vertex in leaders:
        if random.randint(0,1) > 1/2:
            new_reached.append(vertex)
            observation.append(vertex)
        else:
            not_answered.append((vertex, t))
    for vertex in observation:
        neighbors = graph.neighbors(vertex)
        for node in neighbors:
            if node not in new_reached and node not in reached and random.randint(0,1) >= p:
                new_reached.append(node)
    return new_reached, observation, not_answered

# redExecution execute the Red Change solutions to solve the Influence Maximization Problem,
# it returns peer leaders list, the absent subject to calls list the policy (Simulation from a set and its Observation)
# and samples of the probability from a range [0 to 1]
def redExecution(T, K, graph, reached):
    p, table_probabilities = parameterSelection(T, K, graph)
    history = []
    not_answering = []
    for i in range(T):
        A = []
        for j in range(K):
            vertex = selectVertex(A, graph, p, reached, not_answering, i)
            A.append(vertex)
        new_reached,observation, not_answered = makeARun(graph, A, p, reached, i)
        for re in new_reached:
            reached.append(re)
        for el in not_answered:
            not_answering.append(el)
        history.append((A, observation))
    return reached, not_answering, history, table_probabilities