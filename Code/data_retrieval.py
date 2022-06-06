"""
Project: HIV prevention
Version: 1.0
Date: 03-Jun-2022

data_retrieval.py library include functions to manipulate the dataset, Big Data and Constraint Satisfaction Problem solutions.
"""
import os
import ast
import numpy as np
from pyspark.sql.types import StructType, StringType, IntegerType,BooleanType
from datetime import datetime

# controlParam takes in input the number of parameters, the path of the dataset , the number of dropcenters
# and the number of partition, it perform the control of each of these variables :
# 1 - The minimum number of command line parameters have to be 3
# 2 - The dataset must exist in the storage
# 3 - The number of dropcenters and the number of partition have to be integer
# If one of the above is not True the program throws an exception
def controlParam(n_parameters,dataset,n_partitions):
    # Checking number of cmd line parameters
    assert n_parameters == 3, "Usage: python main.py filepath number_of_partitions"
    # Check the if the Dataset exists or not
    assert os.path.isfile(dataset), "File or folder not found"
    # Check if the number of partition is an interger
    assert n_partitions.isdigit() ,"Insert the number of partition"

# divedeDropCenter takes in input the Spark Context , the RDD dataset , the number of partitions and the Drop Center to look for,
# and return the subset of patiences belonging to the Drop Center specified.
def divideDropCenter(sc,dataset,n_partitions,center):
    dataset_list = sc.textFile(dataset, n_partitions).map(lambda line: line.split(";")).repartition(n_partitions).cache()
    dataset_filtered = dataset_list.filter(lambda x : x[11]==str(center))
    return dataset_filtered

# empty check if the list given in input is empty or not
def empty(list):
    size = np.array(list,dtype=object).size
    empty = False
    if size == 0:
        empty = True
    return (empty)

# getSchema return the header of the CSV file to add to the DataFrame version
def getSchema():
    schema =StructType().add("Key",IntegerType(),True)\
                .add("Name Surname",StringType(),True)\
                .add("Country",StringType(),True)\
                .add("Age",IntegerType(),True)\
                .add("Birth sex",StringType(),True)\
                .add("Gender Identity",StringType(),True)\
                .add("Race/Etnicity",StringType(),True)\
                .add("Sexual Orientation",StringType(),True)\
                .add("Sexual Behaviour",StringType(),True)\
                .add("HIV Knowledge",StringType(),True)\
                .add("Living situation",StringType(),True)\
                .add("Drop-in center key",IntegerType(),True)\
                .add("Relationship status",StringType(),True)\
                .add("Drug Experience",StringType(),True)\
                .add("Intervention",StringType(),True)
    return schema

# labelCountry create a dictionary of distinct values of countries and define the labels as value : 
# current index in the list + total size of the list 
def label(list_g):
    size = len(list_g)
    dict = {}
    for i in range(len(list(list_g))):
        dict[list_g[i][0]] = i + size
    return dict

# getDropCenters has as intput the sqlContext and the datapath of the Dataset to select the Distinct Drop Centers from the Dataset
# and return that list
def getDropCenters(sc,dataset):
    # Takes the schema of the CSV file
    schema = getSchema()
    # Create the DataFrame object from the CSV file
    dataframe = sc.read.format("csv") \
      .options(delimiter=';') \
      .schema(schema) \
      .load(dataset)
    # Select the distinct Drop Centers
    drop_centers = dataframe.select("Drop-in center key").distinct().collect()
    interventions = dataframe.select("Intervention").distinct().collect()
    country_list = dataframe.select("Country").distinct().collect()
    birth_sex_list = dataframe.select("Birth sex").distinct().collect()
    gender_identity_list = dataframe.select("Gender Identity").distinct().collect()
    etnicity_list = dataframe.select("Race/Etnicity").distinct().collect()
    sexual_orientation_list = dataframe.select("Sexual Orientation").distinct().collect()
    sexual_behavior_list = dataframe.select("Sexual Behaviour").distinct().collect()
    hiv_knowlegde_list = dataframe.select("HIV Knowledge").distinct().collect()
    relation_status_list = dataframe.select("Relationship status").distinct().collect()
    drug_experience_list = dataframe.select("Drug Experience").distinct().collect()
    dictionaries = []
    country_dict = label(country_list)
    dictionaries.append(country_dict)
    # Add an empty Dictionary for the Age column
    dictionaries.append({}) 
    birth_sex_dict = label(birth_sex_list)
    dictionaries.append(birth_sex_dict)
    gender_identity_dict = label(gender_identity_list)
    dictionaries.append(gender_identity_dict) 
    etnicity_dict = label(etnicity_list) 
    dictionaries.append(etnicity_dict)
    sexual_orientation_dict = label(sexual_orientation_list)
    dictionaries.append(sexual_orientation_dict)
    sexual_behavior_dict = label(sexual_behavior_list)
    dictionaries.append(sexual_behavior_dict)
    hiv_knowlegde_dict = label(hiv_knowlegde_list)
    dictionaries.append(hiv_knowlegde_dict) 
    # Add an empty Dictionaries for the drop_keys column
    dictionaries.append({}) 
    dictionaries.append({})
    relation_status_dict = label(relation_status_list)
    dictionaries.append(relation_status_dict) 
    drug_experience_dict = label(drug_experience_list)
    dictionaries.append(drug_experience_dict) 
    inter_priority = 0
    for intervention in interventions:
        if intervention[0] == "False":
            inter_priority = 1
    drop_centers_list = []
    # Put the Drop Centers into a list
    for center in drop_centers:
        drop_centers_list.append(center[0])
    return drop_centers_list,inter_priority,dictionaries

# analyzeData takes in input the current dataset and return the final dataset
# to use for the maximum influence problem
def analyzeData(dataset,inter_priority):
    # ----------------------------- Round 1 -------------------------------------
    analyzed_dataset = dataset.mapPartitions(lambda x : backtrackSearch(x,inter_priority)).cache()
    return analyzed_dataset 

# createList has the aim of build a pair list (key,list) from the partition of the RDD 
def createList(dataset):
    data_list = []
    for data in dataset :
        data_list.append((data[0],data))
    return data_list

# selectUnassVar select the data that is not included in the final list 
def selectUnassVar(assignment,dataset,dataset_list):
    for data in range(len(dataset)) :
        if dataset[data] not in assignment:
            if (dataset[data][0],dataset[data]) in dataset_list:
                return dataset[data]

# pastDays obtain the difference of date from the stored in the dataset and the current one
def pastDays(date):
    now = datetime.now()
    date = datetime.strptime(date, "%Y-%m-%d")
    return abs((now - date).days)
    

def isConsistent(data,intervention):
    consistent = False
    # Critical domains
    lbgtqi = ["Lesbian","Bisexual","Gay","Transexual","Pansexual"]
    h_inf_countries = ["Eswatini","Lesotho","Botswana","South Africa","Zimbabwe","Namibia","Mozambique"
                        ,"Zambia","Malawi","Equatorial Guinea","South Africa","India","Tanzania","Nigeria"
                        ,"Uganda","Kenya","Zimbabwe","Russia"]
    age_threshold = 26
    hiv_knowledge = ["poor","None"]
    drug_past = True
    sexual_behaviour = ["NCAS","NCVS"]
    relationship_status = ["Dating","Married"]
    # Days of waiting before another intervention
    intervention_threshold = 30
    # Order of critical cases :
    if intervention != 0 :
        # Patiences which have the combination of no-protect Intercourse or Drug abuse and lack of knowledge of HIV and belongs to LBGTQI+ community
        if str(data[8]) in sexual_behaviour or str(data[13]) == drug_past and str(data[9]) in hiv_knowledge and str(data[7]) in lbgtqi:
            return True
        # Patiences with combination of no-protect Intercourse and is engaged with someone
        elif str(data[8]) in sexual_behaviour and str(data[12]) in relationship_status :
            return True
        # Patiences whose Country of Birth is one whose infection of HIV cases are warning , absence or poor HIV knowledge and combination of no-protect Intercourse
        # and Drug abuse
        elif str(data[2]) in h_inf_countries and str(data[9]) in hiv_knowledge and str(data[8]) in sexual_behaviour or str(data[13]) == drug_past:
            return True
        # Patiences whose Country of Birth is one whose infection of HIV cases are warning,combination of no-protect Intercourse
        # and Drug abuse
        elif str(data[2]) in h_inf_countries and str(data[8]) in sexual_behaviour or str(data[13]) == drug_past :
            return True
        # Patiences whose age is less than the age_threshold, lack of HIV knowledge and has past on Drug and had no-protect Intercourse
        elif int(data[3]) <= age_threshold and str(data[9]) in hiv_knowledge or str(data[8]) in sexual_behaviour or str(data[13]) == drug_past:
            return True
    elif data[14] != "False" :
        Inter_list = ast.literal_eval(data[14])
        # Compute the past days from the last intervention
        days = pastDays(Inter_list[1])
        # if the above difference is > than the intervention_threshold then will be included
        if days >= intervention_threshold:
            return True
    return consistent

# Call of the backtrack algorithm
def backtrackSearch(dataset,inter_priority):
    d = list(dataset)
    if len(d) == 0:
        return []
    else :
        dataset_list = createList(d)
        result = backtrack(d,dataset_list,[],inter_priority)
        return result

# backtrack takes in input the partitioned subset , the list of element of the subset
# , the list where will be stored the result of the backtrack search and 
# intervention priority , where if is 0 it restart the post intervention process
# only for the patience with >= 30 days after their previous intervention
def backtrack(dataset,dataset_list,assignment,inter_priority):
    # check if the search is completed 
    if empty(dataset_list):
        return assignment    
    # select the unassignment variable from the iterable
    data = selectUnassVar(assignment,dataset,dataset_list)
    # check if the variables are consistent
    if isConsistent(data,inter_priority) is True:
        # Add the row to the final dataset
        assignment.append(data)
        # Remove the row from the list of the dataset
        dataset_list.remove((data[0],data))
        # recursion call to backtrack
        result = backtrack(dataset,dataset_list,assignment,inter_priority)
        if result is not None :
            return result
    # In case of non consistent value continue to search
    else :
        # Remove the row from the list of the dataset
        dataset_list.remove((data[0],data))
        result = backtrack(dataset,dataset_list,assignment,inter_priority)
        return result

# createDict has the aim of build a dictionary from the partition of the RDD 
def createDict(dataset):
    data_dict = {}
    for data in dataset :
        data_dict[data[0]] = data
    return data_dict

def current_date():
    current_date = datetime.now()
    year = current_date.year
    month = current_date.month
    day = current_date.day
    date_format = str(year)+"-"+str(month)+"-"+str(day)
    return date_format

# updateIntervention has as input the list of peer leaders and the list of the not present 
# and the RDD contains the data of the current drop-in center 
# calls the update function for each partitions of the drop-in center sets,
# it return the RDD of the updated datasets
def updateRDD(leaders,absent,dataset):
    # ------------------------- Round 1 ---------------------------------------
    update = dataset.mapPartitions(lambda x : updateSet(x,leaders,absent)).cache()
    return update

def updateInter(intervention,modality):
    if type(intervention) == list:
        # Check for the last intervention
        date_intervention = intervention[1]
        # If the subject have been already scheduled return it
        if date_intervention == current_date():
            return intervention
        # If is an old intervention Update
        else:
            # Update the number of intervention
            num_intervention = intervention[0]
            if modality == "leader":
                num_intervention += 1
            else :
                num_intervention = -1
            # Get the current date in format Y/mm/dd
            date_int = current_date()
            new_intervention = [num_intervention,date_int]
    else :
        # Create the new number of intervention
        if modality == "leader":
            num_intervention = 1
        else :
            num_intervention = -1
        date_int = current_date()
        new_intervention = [num_intervention,date_int]
    return new_intervention

# update takes as input the iterator of the partition of a set of the current drop-in center ,
# the list of peer leaders and absents , it produce as output the list if the updated dataset 
def updateSet(data,leaders,absent):
    d = list(data)
    if len(d) == 0:
        return []
    else :
        dataset_dict = createDict(d)
        # Update the peer leaders Intervention as a list [number of intervention, current_date_of_intervention]
        for i in range(len(leaders)):
            for j in leaders[i]:
                if j[0] in dataset_dict.keys():
                    subject = dataset_dict[j[0]] 
                    intervention = subject[14]
                    new_intervention = updateInter(intervention,"leader")
                    # Update the Intervention for the subject
                    subject[14] = new_intervention
                    dataset_dict[j[0]] = subject
        # The absent peer leaders will have a list of [-1 , current_date_of_the_absence]
        for i in range(len(absent)):
            for j in absent[i]:
                if j[0][0] in dataset_dict.keys():
                    subject = dataset_dict[j[0][0]]
                    intervention = subject[14]
                    new_intervention = updateInter(intervention,"absent")
                    # Update the Intervention for the subject
                    subject[14] = new_intervention
                    dataset_dict[j[0][0]] = subject
        # Convert dictionary to list
        return [value for value in dataset_dict.values()]
    