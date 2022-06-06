"""
Project: HIV prevention
Version: 1.0
Date: 03-Jun-2022

main.py library include the calls for the execution of the experiment.
"""
import sys
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
import findspark
from data_retrieval import *
from red_change import *
from save_results import *

# Find pyspark in the system , only for local machines. 
findspark.init()

def main():
    
    # Get the input from the Command Line
    budget = 8
    n_parameters = len(sys.argv)
    dataset_path = sys.argv[1]
    n_partitions = sys.argv[2]

    # Evaluate the correctness of all the previous variables
    controlParam(n_parameters,dataset_path,n_partitions)
    #n_drop_center = int(n_drop_center)
    n_partitions = int(n_partitions)
    
    # Create and Configure a Spark Session and SQL Spark Session
    conf = SparkConf().setAppName('Maximum Influence AI Project')
    sc = SparkContext(conf=conf)
    sc.setLogLevel("WARN")
    sqls = SparkSession.builder.getOrCreate()

    # Obtain the Drop Centers
    drop_center_list,inter_priority,dict = getDropCenters(sqls,dataset_path)
    # Check if there are Drop Centers
    assert len(drop_center_list) != 0 , "No Drop Center in the Dataset"
    # Obtain the RDDs list 
    dataset = []
    # If the number of Drop Centers is more than 1 collect an array of RDD with subset of the Dataset
    # splitted with each patiences belonging to the respective Drop Center
    if len(drop_center_list) > 1:
        for i in drop_center_list:
            subset = divideDropCenter(sc,dataset_path,n_partitions,i)
            subset_list = subset.collect()
            if empty(subset_list) is False:
                dataset.append(subset)
    else :
        d_set = sc.textFile(dataset_path, n_partitions).map(lambda line: line.split(";")).repartition(n_partitions).cache()
        dataset.append(d_set)
    
    # Analyze the datasets 
    starting_set = []
    for i in range(len(dataset)):
        starting_set.append(analyzeData(dataset[i],inter_priority))

    # Create the Social Network
    networks = createNetworks(starting_set, dict)
    
    # Solve the Maximum Influencial Problem
    reached_in_networks = []
    not_answering_in_networks = []
    history_in_networks = []
    table_probabilities_in_networks = []
    for network in networks:
        graph = networkSamplingPlus(network, budget)
        reached, not_answering, history, table_probabilities = redExecution(3, 3, graph, [])
        reached = tuple(reached)
        reached_in_networks.append(reached)
        not_answering = tuple(not_answering)
        not_answering_in_networks.append(not_answering)
        history = tuple(history)
        history_in_networks.append(history)
        table_probabilities = tuple(table_probabilities)
        table_probabilities_in_networks.append(table_probabilities)
    
    # Update the database
    new_dataset = []
    for i in range(len(dataset)):
        new_dataset.append(updateRDD(reached_in_networks,not_answering_in_networks,dataset[i]))
    
    # Update the dataset and manage some Satistics
    for i_set in  range(len(dataset)): 
        # Save the Pie chart of the HIV knowledge per Country's subjects
        filt_columns = ["HIV Knowledge"]
        createPieExcelFile(dataset[i_set] ,drop_center_list[i_set],"Country",filt_columns,"Pie "+str(i_set)+" HIV analysis")
        # Save the Updated dataset into an Excel file
        saveData(list(new_dataset[i_set].collect()) , drop_center_list[i_set])
        # Save the Updated dataset into CSV format
        saveCSVData(list(new_dataset[i_set].collect()) , drop_center_list[i_set])
    
# Just start the main program
if __name__ == "__main__":
    main()