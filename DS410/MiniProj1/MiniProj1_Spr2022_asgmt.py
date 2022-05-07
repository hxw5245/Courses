#!/usr/bin/env python
# coding: utf-8

# # DS/CMPSC 410 MiniProject Deliverable #1 
# 
# # Spring 2022
# ## Instructor: John Yen
# ## TA: Rupesh Prajapati
# ## LA: Lily Jakielaszek and Cayla Shan Pun
# ## Learning Objectives
# - Be able to identify frequent 2 port sets and 3 port sets that are scanned by scanners in the Darknet dataset
# - Be able to adapt the Aprior algorithm by incorporating suitable threshold.
# - Be able to improve the performance of frequent port set mining by suitable reuse of RDD, together with appropriate persist and unpersist on the reused RDD.
# - Be able to enhance the performance of mining frequent port from a Big Dataset using persist.
# 
# ### Total points: 100 
# - Exercise 1: 10 points
# - Exercise 2: 10 points
# - Exercise 3: 5 points
# - Exercise 4: 15 points
# - Exercise 5: 20 points
# - Exercise 6: 10 points
# - Exercise 7: 30 points
#   
# ### Due: midnight, April 1, 2022

# In[1]:


import pyspark
import csv
import pandas as pd


# In[2]:


from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, LongType
from pyspark.sql.functions import col, column
from pyspark.sql.functions import expr
from pyspark.sql.functions import split
from pyspark.sql import Row
from pyspark.ml import Pipeline
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler, IndexToString
from pyspark.ml.clustering import KMeans


# In[3]:


ss = SparkSession.builder.appName("Mini Project #1 Freqent Port Sets").getOrCreate()


# # Exercise 1 (10 points)
# - Complete the path below for reading "sampled_profile.csv" you downloaded from Canvas, uploaded to your Mini Project 1 folder. (5 points)
# - Fill in your Name (5 points): Haichen Wei

# In[4]:


Scanners_df = ss.read.csv("/storage/home/hxw5245/MiniProj1/Day_2020_profile.csv", header=True, inferSchema=True)



# ## Convert the Column 'ports_scanned_str' into an Array of ports scanned by each scanner (row)

# In[7]:


Scanners_df2=Scanners_df.withColumn("Ports_Array", split(col("ports_scanned_str"), "-") )



# In[9]:


Ports_Scanned_RDD = Scanners_df2.select("Ports_Array").rdd



# # Convert an RDD of Ports_Array into an RDD of port list.
# ## Because the Row object in the RDD only has Ports_Array, we can access the content of Ports_Array using index 0.
# ## The created RDD `multi_Ports_list_RDD` will be used in finding frequent port sets below (Part B).

# In[11]:


multi_Ports_list_RDD = Ports_Scanned_RDD.map(lambda x: x[0])



# # Part B: Finding all ports that have been scanned by at least 1000 scanners.

# ## Because each port number in the Ports_Array column for each row/scanner occurs only once, we can count the total occurance of each port number through flatMap.

# ## `flatMap` flatten the RDD into a list of ports. We can then just count the occurance of each port in the RDD, which is the number of scanners that scan the port.   

# # Exercise 2 (10 points) Complete the code below to calculate the total number of scanners that scan each port. 

# In[13]:


port_list_RDD = multi_Ports_list_RDD.flatMap(lambda x: x)


# In[14]:


Port_count_RDD = port_list_RDD.map(lambda x: (x,1) )


# In[15]:


Port_count_total_RDD = Port_count_RDD.reduceByKey(lambda x,y: x+y, 1)




# In[17]:


Sorted_Count_Port_RDD = Port_count_total_RDD.map(lambda x: (x[1], x[0])).sortByKey( ascending = False)




# ## Since we are interested in ports that are scanned by at least 1000 scanners.  We can use 999 as the threshold.
# ## Exercise 3 (5 points) Complete the following code to filter for ports that have at least 1000 scanners.

# In[19]:


threshold = 4999
Filtered_Sorted_Count_Port_RDD= Sorted_Count_Port_RDD.filter(lambda x: x[0] > threshold)




# # After we apply collect to the RDD, we get a list of single ports that are scanned by at least 1000 scanners in the small dataset.

# In[21]:


Top_Ports = Filtered_Sorted_Count_Port_RDD.map(lambda x: x[1]).collect()


# In[22]:


Top_1_Port_count = len(Top_Ports)





# # Part D: Finding Frequent 2-Port Sets and 3-Port Sets

# # Exercise 5 (20 points)  Modify and complete the following code to find BOTH frequent 2 port sets AND frequent 3 port sets 
# ## Hint 1: Add the code of saving frequent two port sets in Pandas dataframe `Two_Port_Sets_df` (similar to those code in the previous Exercise).
# ## Hint 2: Need to have two `index` variables. One for each Pandas dataframe.

# In[29]:


# Initialize a Pandas DataFrame to store frequent port sets and their counts
Two_Port_Sets_df = pd.DataFrame( columns= ['Port Sets', 'count'])
Three_Port_Sets_df = pd.DataFrame( columns= ['Port Sets', 'count'])
# Initialize the index to the Three_Port_Sets_df to 0
index2 = 0
index3 = 0
# Set the threshold for Large Port Sets to be 1000
threshold = 4999
for i in range(0, Top_1_Port_count-1):
    Scanners_port_i_RDD = multi_Ports_list_RDD.filter(lambda x: Top_Ports[i] in x)
    Scanners_port_i_RDD.persist()  
    for j in range(i+1, Top_1_Port_count-1):
        Scanners_port_i_j_RDD = Scanners_port_i_RDD.filter(lambda x: Top_Ports[j] in x)
        Scanners_port_i_j_RDD.persist()
        two_ports_count = Scanners_port_i_j_RDD.count()
        if two_ports_count > threshold:
            Two_Port_Sets_df.loc[index2]=[ [Top_Ports[i], Top_Ports[j]], two_ports_count]
            index2 = index2 +1
            print("Two Ports: ", Top_Ports[i], ", ", Top_Ports[j], ": Count ", two_ports_count)
            for k in range(j+1, Top_1_Port_count -1):
                Scanners_port_i_j_k_RDD = Scanners_port_i_j_RDD.filter(lambda x: Top_Ports[k] in x)
                three_ports_count = Scanners_port_i_j_k_RDD.count()
                if three_ports_count > threshold:
                    Three_Port_Sets_df.loc[index3] = [ [Top_Ports[i], Top_Ports[j], Top_Ports[k]], three_ports_count]
                    index3 = index3 + 1
                    print("Three Ports: ", Top_Ports[i], ", ", Top_Ports[j], ",  ", Top_Ports[k], ": Count ", three_ports_count)
        Scanners_port_i_j_RDD.unpersist()
    Scanners_port_i_RDD.unpersist()


# # Convert the Pandas dataframes into PySpark DataFrame

# In[30]:


Two_Port_Sets_DF = ss.createDataFrame(Two_Port_Sets_df)
Three_Port_Sets_DF = ss.createDataFrame(Three_Port_Sets_df)


# # Exercise 6 (10 points)
# Complete the following code to save your frequent 2 port sets and 3 port sets in an output file.

# In[31]:


output_path_2_port = "/storage/home/hxw5245/MiniProj1/2Ports"
output_path_3_port = "/storage/home/hxw5245/MiniProj1/3Ports"
Two_Port_Sets_DF.rdd.saveAsTextFile(output_path_2_port)
Three_Port_Sets_DF.rdd.saveAsTextFile(output_path_3_port)


# # Exercise 7 (30 points)
# - Remove .master("local") from SparkSession statement
# - Change the input file to "/gpfs/group/juy1/default/private/Day_2021_profile.csv"
# - Remove part C. 
# - Change the output files to two different directories from the ones you used in Exercise 5
# - Export the notebook as a .py file
# - Run spark-submit on ICDS Roar 
# - Record the performance time.

# In[ ]:




