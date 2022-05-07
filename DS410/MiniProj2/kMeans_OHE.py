#!/usr/bin/env python
# coding: utf-8

# # DS/CMPSC 410 MiniProject Deliverable #2
# 
# # Spring 2022
# ### Instructor: Prof. John Yen
# ### TA: Rupesh Prajapati
# ### LA: Lily Jakielaszek and Cayla Shan Pun
# 
# ### Learning Objectives
# - Be able to apply k-means clustering to the Darknet dataset.
# - Be able to identify the set of top k ports for one-hot encoding ports scanned.
# - Be able to characterize generated clusters using cluster centers.
# - Be able to compare and evaluate the result of k-means clustering with different features using Silhouette score and external labels.
# 
# ### Total points: 100 
# - Exercise 1: 5 points
# - Exercise 2: 5 points 
# - Exercise 3: 10 points 
# - Exercise 4: 5 points
# - Exercise 5: 10 points
# - Exercise 6: 10 points
# - Exercise 7: 15 points
# - Exercise 8: 10 points
# - Exercise 9: 30 points
#   
# ### Due: 11:59 pm, April 10, 2022

# In[1]:


import pyspark
import csv


# In[2]:


from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, LongType
from pyspark.sql.functions import col, column
from pyspark.sql.functions import expr
from pyspark.sql.functions import split
from pyspark.sql.functions import array_contains
from pyspark.sql import Row
from pyspark.ml import Pipeline
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler, IndexToString
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator


# In[3]:


ss = SparkSession.builder.appName("MiniProject 2 Clustering OHE").getOrCreate()


# ## Exercise 1 (5 points)
# Complete the path for input file in the code below and enter your name in this Markdown cell:
# - Name: Haichen Wei

# In[4]:


Scanners_df = ss.read.csv("/storage/home/hxw5245/MiniProj1/Day_2020_profile.csv", header= True, inferSchema=True )




# # Exercise 1: Write Your Name Below: (5 points)
# ## Solution to Exercise 1: Haichen Wei

# # Part A: One Hot Encoding of Top 50 Ports
# We want to apply one hot encoding to the top k set of ports scanned by scanners. 
# - A1: Find top k ports scanned by most scanners (This is similar to the first part of MiniProject 1)
# - A2: Generate One Hot Encodding for these top k ports




# # Count the Total Number of Scanners, regardless how many ports they scan, that Scan a Given Port
# Like MiniProject 1, to calculate this, we need to 
# - (a) convert the ports_scanned_str into an array/list of ports
# - (b) Convert the DataFrame into an RDD
# - (c) Use flatMap to count the total number of scanners for each port.

# # The Following Code Implements the three steps.
# ## (a) Split the column "Ports_Array" into an Array of ports.

# In[7]:


# (a)
Scanners_df2=Scanners_df.withColumn("Ports_Array", split(col("ports_scanned_str"), "-") )


# ## (b) We convert the column ```Ports_Array``` into an RDD so that we can apply flatMap for counting.

# In[8]:


Ports_Scanned_RDD = Scanners_df2.select("Ports_Array").rdd


# ## II.(c) Because each port number in the Ports_Array column for each row/scanner occurs only once, we can count the total number of scanners by counting the total occurance of each port number through flatMap.
# ### Because each element of the rdd is a Row object, we need to first extract the first element of the row object, which is the list of Ports, from the ``Ports_Scanned_RDD``
# ### We can then count the total number of occurance of a port using map and reduceByKey, like counting word/hashtag frequency in tweets.

# In[10]:


# This step is for demonstration purpose only.
Ports_list_RDD = Ports_Scanned_RDD.map(lambda row: row[0] )



# In[12]:


Ports_list2_RDD = Ports_Scanned_RDD.flatMap(lambda row: row[0] )




# In[14]:


Port_count_RDD = Ports_list2_RDD.map(lambda x: (x, 1))



# In[15]:


Port_count_total_RDD = Port_count_RDD.reduceByKey(lambda x,y: x+y)


# # Exercise 2 (5%)
# ### Find the total number of ports being scanned by 
# - (a) completing the code below

# In[16]:


# Port_count_total_RDD.count()


# # Exercise 3 (10 points)
# ### Complete the code below for finding top 50 ports.

# In[17]:


Sorted_Count_Port_RDD = Port_count_total_RDD.map(lambda x: (x[1], x[0])).sortByKey( ascending = False)




# In[19]:


top_ports= 50
Sorted_Ports_RDD= Sorted_Count_Port_RDD.map(lambda x: x[1] )
Top_Ports_list = Sorted_Ports_RDD.take(top_ports)


# In[20]:


# Top_Ports_list


# #  A.2 One Hot Encoding of Top K Ports
# ## One-Hot-Encoded Feature/Column Name
# Because we need to create a name for each one-hot-encoded feature, which is one of the top k ports, we can adopt the convention that the column name is "PortXXXX", where "XXXX" is a port number. This can be done by concatenating two strings using ``+``.

# In[21]:


# Top_Ports_list[0]


# In[22]:


FeatureName = "Port"+Top_Ports_list[0]


# In[23]:


# FeatureName


# ## One-Hot-Encoding using withColumn and array_contains

# In[24]:


from pyspark.sql.functions import array_contains


# In[25]:


Scanners_df3=Scanners_df2.withColumn(FeatureName, array_contains("Ports_Array", Top_Ports_list[0]))




# ## Verify the Correctness of One-Hot-Encoded Feature
# ## Exercise 4 (5 points)
# ### Check whether one-hot encoding of the first top port is encoded correctly by completing the code below and enter your answer the in the next Markdown cell.

# In[32]:


# First_top_port_scanners_count = Scanners_df3.where(col("Port17132") == True).rdd.count()




# ## Answer for Exercise 4:
# - The total number of scanners that scan the first top port, based on ``Sorted_Count_Port_RDD`` is: 32014.
# - Is this number the same as the number of scanners whose One-Hot-Encoded feature of the first top port is True?
# 
#     It is the same number as One-Hot-Encoded feature of the first top port is True.

# ## Generate Hot-One Encoded Feature for each of the top k ports in the Top_Ports_list
# 
# - Iterate through the Top_Ports_list so that each top port is one-hot encoded.

# ## Exercise 5 (10 points)
# Complete the following PySpark code for encoding the n ports using One Hot Encoding, where n is specified by the variable ```top_ports```

# In[34]:


# top_ports


# In[35]:


# Top_Ports_list[49]


# In[36]:


for i in range(0, top_ports):
    # "Port" + Top_Ports_list[i]  is the name of each new feature created through One Hot Encoding
    Scanners_df3 = Scanners_df2.withColumn("Port" + Top_Ports_list[i], array_contains("Ports_Array", Top_Ports_list[i]))
    Scanners_df2 = Scanners_df3




# ## Exercise 6 (10 points)
# Complete the code below to use k-means to cluster the scanners using one-hot-encoded top 50 ports and the following two  features:
# - lifetime  : The average lifetime of scanners.
# - Packets   : The average number of packets scanned by each scanner.

# ## Specify Parameters for k Means Clustering

# In[38]:


input_features = [ "lifetime", "Packets"]
for i in range(0, top_ports ):
    input_features.append( "Port"+ Top_Ports_list[i] )




# In[40]:


va = VectorAssembler().setInputCols(input_features).setOutputCol("features")


# In[41]:


data= va.transform(Scanners_df2)




# In[43]:


data.persist()


# In[44]:


km = KMeans(featuresCol= "features", predictionCol="prediction").setK(100).setSeed(123)
km.explainParams()


# In[45]:


kmModel=km.fit(data)


# In[46]:


# kmModel


# In[47]:


predictions = kmModel.transform(data)



# In[49]:


Cluster1_df=predictions.where(col("prediction")==0)


# In[50]:


Cluster1_df.count()


# In[51]:


summary = kmModel.summary


# In[52]:


summary.clusterSizes


# In[53]:


evaluator = ClusteringEvaluator()
silhouette = evaluator.evaluate(predictions)


# In[54]:


# print('Silhouette Score of the Clustering Result is ', silhouette)


# In[55]:


centers = kmModel.clusterCenters()


# In[56]:


# print(centers)


# In[57]:


sc = ss.sparkContext


# In[58]:


centers_rdd = sc.parallelize(centers)


# In[59]:


centers_rdd.saveAsTextFile("MiniProject_2B_ClusterResult")


# ## Exercise 7 Complete the code below to perform k-means clustering using only One Hot Encoded Features (15%)

# In[60]:


input_features2 = [ ]
for i in range(0, top_ports ):
    input_features2.append( "Port"+ Top_Ports_list[i] )




# In[62]:


va2 = VectorAssembler().setInputCols(input_features2).setOutputCol("features2")


# In[63]:


data2= va2.transform(Scanners_df2)


# In[64]:


data2.persist()




# In[66]:


km2 = KMeans(featuresCol="features2", predictionCol="prediction2").setK(100).setSeed(123)
km2.explainParams()


# In[67]:


kmModel2=km2.fit(data2)


# In[68]:


# kmModel2


# In[69]:


predictions2 = kmModel2.transform(data2)


# In[70]:


predictions2.persist()


# In[71]:


summary2 = kmModel2.summary


# In[72]:


summary2.clusterSizes


# In[73]:


evaluator2 = ClusteringEvaluator(featuresCol='features2', predictionCol='prediction2')
silhouette2 = evaluator2.evaluate(predictions2)


# In[74]:


# print('Silhouette Score of the Clustering Result is ', silhouette2)


# In[75]:


centers2 = kmModel2.clusterCenters()


# In[76]:


centers2_rdd = sc.parallelize(centers2)


# In[77]:


centers2_rdd.saveAsTextFile("MiniProject_2B_Cluster_Centers_Only_OHE")


# # Exercise 8 (10 points) 
# - (a) Compare the clutering results of the two approaches above (1) OHE + three numerical features, and (2) OHE.  (5 points)
# - (b) Discuss the reasons one approach is worse than the other. (5 points)

# # Answer to Exercise 8:
# - (a) OHE+features' Clustering Result is  0.9083484568569015. The OHE Clustering Result is  0.7394072054440451. The OHE+features is better. 
# - (b) Selecting specific features can help to find clusters more efficiently, understand the data better, and reduce data size for storage, collection, and processing. 

# # Exercise 9 (30 points)
# Modify the Jupyter Notebook for running in cluster mode using the big dataset (Day_2020_profile.csv). 
# Submit the .py file using spark-submit in the cluster mode to calculate cluster centers of the two different approaches (one using OHE + numerical features, the other using only OHE).
# - Submit the .py file and the log file that contains the run time information.
# - Submit a screen shot showing the output directories (both in local mode and in cluster mode)
# - Submit the output files for each approach in the cluster mode.

# In[ ]:


ss.stop()

