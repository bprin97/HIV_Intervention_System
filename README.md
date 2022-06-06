# HIV_Intervention_System

The Influence Maximization Problem consists in the identification of the most influenctial nodes in a Social Graph under study, also referred as an Optimization Problem that is NP-Hard.

In this case the we takes into account the challange of raising awareness about HIV among homeless youth , and given subjects from different drop-in centers they will be trained as peer leaders who communicate with other youth about HIV prevention. And from here which one will be the peer leaders that reachs a great number of youths?
From this question is designed an Indipendent Multi-Agent system where are : Drop-in Centers-AI software which can handle a huge amount of data from which is going to solve the Influential Maximization Problem for each drop-in center.

It is well-known the lacks of the data when is considered to work for a project of an Artificial Intelligence for Social Good, several the reasons privacy concerns and the cost to collect it is incommensurable but most of the times expensive both in resources and time. The HIV intervention field is not so far behind , the methods could be the adoption of surveys , much easier if retrieved with an Automatize Online Framework to store them before the usage of the HIV_Intervention_System.
Despite that in our case we adopted 2 Datasets either Self-Made : HIV_Dataset_1_Center.csv which contains 210 subjects from only 1 drop-in center and HIV_Dataset_2_Centers.csv with subjects from 3 different drop-in centers.

The social network is represented by a Graph G = (V,E) where the nodes are the subjects , our solution is modelled by a variant of the Indipendent Cascade Model where the entire process of selection condidered a discrete sampled period of time [t T] = t1, t2 , t3 , t4 , .... T, and exploits the methods we termed REDCHANGE which selects fairly the set of peer leaders. REDCHANGE stands for faiR randomizED CompreHensive Adaptive Network samplinG for social influencE , it is a variant of the CHANGE approach [Wilder 2018b , Wilder 2021] where now the Network Sampling section use a K-Means++ approach.
Morover the entire solution is thought to handle Big Data and the postIntervention selection.

# Our Solution in Summary :

The algorithm is dividen into 3 steps : 

1) Data Manipulation (Big Data and Constraint Satisfation Problem approaches) (files : data_retrieval.py)
2) Influence Maximization Solution (Influence Maximization and Clustering Problem approach = REDCHANGE) (files : red_change.py)
3) Statistics ans Storage Data (Create Statistics of the HIV knowledge per Country with charts and save the updated dataset) (files : save_results.py)

## Running the Experiments :

In order to run the application :

1) Install the requirement packages from the requirements.txt
2) Open the Terminal 
3) Run it with the following command : python main.py "path_to_one_of_the_datasets" number of partition (e.g python main.py "/Datasets/HIV_Dataset_2_Centers.csv" 2)

## Computational Time

time required to the algorithm :
1) without the statistics part (function createPieExcelFile) : milliseconds
2) with the statistics part (function createPieExcelFile) : around 10 minutes

So be aware if you want to test the effectiveness of the solutions without compute the statistics of the HIV Knowledge comment the line 90 in the main.py, then the computations to solve the problem will be just few milliseconds rather the creation of the charts from the function createPieExcelFile will require additional ten minutes.

## Results :

At the end of each running the resulting Statistics and Updated Datasets for every drop-in centers are found in the folder Results.
You can also find some old results always from that folder : 

### Statistics files from drop-in center : "1" , "2" , "None" derived the 05/06/2022
1) chart_pie_1_2022_6_5 in the Excel format
2) chart_pie_2_2022_6_5 in the Excel format
3) chart_pie_None_2022_6_5 in the Excel format

### Updated datasets for drop-in center : "1" , "2" , "None" derived the 06/06/2022
1) data_1_2022_6_6 in the CSV and Excel format
2) data_2_2022_6_6 in the CSV and Excel format
3) data_None_2022_6_6 in the CSV and Excel format


