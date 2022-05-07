#!/usr/bin/env python
# coding: utf-8

# # DS/CMPSC 410 MiniProject #3
# # Spring 2022
# ## Instructor: Professor John Yen
# ## TA: Rupesh Prajapati 
# ## LAs: Lily Jakielaszek and Cayla Shan Pun
# 
# ### Learning Objectives
# - Be able to apply bucketing to numerical variables for their integration with OHE features.
# - Be able to apply k-means clustering to the Darknet dataset by combining buketing and one-hot encoding, first in local mode, then in cluster mode using the big data.
# - Be able to use external labels (e.g., mirai) to evaluate and compare the results of k-means clustering.
# - Be able to compare different mirai clusters identified by different k-means clustering and gain insights about characteristics of the clusters.  
# 
# ### Total points: 100 
# - Exercise 1: 5 points
# - Exercise 2: 10 points 
# - Exercise 3: 15 points 
# - Exercise 4: 5 points
# - Exercise 5: 10 points
# - Exercise 6: 5 points
# - Exercise 7: 5 points
# - Exercise 8: 10 points
# - Exercise 9: 15 points
# - Exercise 10: 20 points
#   
# ### Due: 11:59 pm, April 17th, 2022

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
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler, IndexToString, PCA
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator


# In[3]:


import pandas as pd
import numpy as np
import math


# In[4]:


ss = SparkSession.builder.appName("MiniProject #3").getOrCreate()


# # Exercise 1 (5 points)
# Enter your name in this Markdown cell and complete the path for your data:
# - Name: Haichen Wei




# ## The higest value for the "Packets" column is 29,924,992 (close to 30 million). The minimum value is 1.

# In[8]:


Scanners_df = ss.read.csv("/storage/home/hxw5245/MiniProj1/Day_2020_profile.csv", header= True, inferSchema=True )


# ## We can use printSchema() to display the schema of the DataFrame Scanners_df to see whether it was inferred correctly.

# In[9]:


# Scanners_df.printSchema()


# ## Part A Compute the Correlation among features.
# ### Reason: Using two highly correlated features is (almost) like using a feature twice -- It can bias the clustering result because the distance calculation gives the feature more weight.

# In[10]:


Scanners_df.stat.corr("Packets", "Bytes")


# In[11]:


from pyspark.sql.functions import corr
# Scanners_df.select(corr("Packets", "Bytes")).show()


# In[12]:


# Scanners_df.select(corr("lifetime", "Packets")).show()


# In[13]:


# Scanners_df.select(corr("lifetime", "numports")).show()


# In[14]:


# Scanners_df.select(corr("numports", "MinUniqueDests")).show()


# # Part B: Transforming Numerical Features Using Bucketing

# ## B.1 Bucketing
# Transform a numerical feature into multiple "buckets" by specifying their boundaries.
# - A benefit of this transformation is controlling the maximal distance of this feature (so that it does not overweigh other features such as One-Hot-Encoded features)
# 

# In[15]:


# Scanners_df.select("Packets").describe().show()


# ## Even though the highest value of "Packets" column for this smaller scanner dataset is 23,726,033.  We have found earlier that the higest value of "Packets" column for the big scanner dataset is close to 30 million.

# In[16]:


Packets_RDD=Scanners_df.select("Packets").rdd


# In[17]:


Packets_rdd = Packets_RDD.map(lambda row: row[0])


# In[18]:


Packets_rdd.histogram([0,10,100,1000,10000,100000,1000000,10000000,100000000])


# # Exercise 2 (10 points)
# Complete the code below to convert the feature ``Packets`` into 10 buckets (based on 11 borders in ``bucketBorders``)

# In[19]:


from pyspark.ml.feature import Bucketizer
bucketBorders=[-1.0, 5.0, 10.0, 25.0, 50.0, 100.0, 1000.0, 10000.0, 100000.0, 1000000.0, 100000000.0]
bucketer = Bucketizer().setSplits(bucketBorders).setInputCol("Packets").setOutputCol("Packets_B10")
Scanners2_df = bucketer.transform(Scanners_df)


# In[20]:


# Scanners2_df.printSchema()


# In[21]:


# Scanners2_df.select("Packets","Packets_B10").where("Packets > 100000").show(30)


# In[22]:


# Scanners2_df.select("numports").describe().show()


# In[23]:


# Scanners_df.where(col('mirai')).count()


# # Part C: One Hot Encoding 
# ## This part is identical to that of Miniproject Deliverable #2
# We want to apply one hot encoding to the set of ports scanned by scanners.  
# - C.1 Like Mini Project deliverable 1 and 2, we first convert the feature "ports_scanned_str" to a feature that is an Array of ports
# - C.2 We then calculate the total number of scanners for each port
# - C.3 We identify the top n port to use for one-hot encoding (You choose the number n).
# - C.4 Generate one-hot encoded feature for these top n ports.

# In[ ]:


# Scanners_df.select("ports_scanned_str").show(30)


# In[24]:


Scanners3_df=Scanners2_df.withColumn("Ports_Array", split(col("ports_scanned_str"), "-") )
# Scanners_df2.persist().show(10)


# # C.1 Convert the `ports_scanned_str` column to an array
# ## We only need the column ```Ports_Array``` to calculate the top ports being scanned

# In[25]:


Ports_Scanned_RDD = Scanners3_df.select("Ports_Array").rdd


# In[ ]:


# Ports_Scanned_RDD.persist().take(5)


# # C.2 Calculate the Total Number of scanners for each port
# ### Because each port number in the Ports_Array column for each row occurs only once, we can count the total occurance of each port number through flatMap.

# In[26]:


Ports_list_RDD = Ports_Scanned_RDD.map(lambda row: row[0] )


# In[ ]:


# Ports_list_RDD.persist()


# In[27]:


Ports_list2_RDD = Ports_Scanned_RDD.flatMap(lambda row: row[0] )


# In[28]:


Port_count_RDD = Ports_list2_RDD.map(lambda x: (x, 1))
# Port_count_RDD.take(2)


# In[29]:


Port_count_total_RDD = Port_count_RDD.reduceByKey(lambda x,y: x+y, 1)
# Port_count_total_RDD.persist().take(5)


# In[30]:


Sorted_Count_Port_RDD = Port_count_total_RDD.map(lambda x: (x[1], x[0])).sortByKey( ascending = False)


# In[ ]:


# Sorted_Count_Port_RDD.persist().take(50)


# # C.3 Identify top n ports to use for one-hot encoding.
# ### Select top_ports to be the number of top ports you want to use for one-hot encoding.  I recommend a number between 20 and 60.

# In[31]:


top_ports= 50
Sorted_Ports_RDD= Sorted_Count_Port_RDD.map(lambda x: x[1])
Top_Ports_list = Sorted_Ports_RDD.take(top_ports)


# In[32]:


Top_Ports_list


# In[ ]:


# Scanners_df3=Scanners_df2.withColumn(FeatureName, array_contains("Ports_Array", Top_Ports_list[0]))


# In[ ]:


# Scanners_df3.show(10)


# # C.4 Generate Hot-One Encoded Feature for each of the top ports in the Top_Ports_list
# 
# - Iterate through the Top_Ports_list so that each top port is one-hot encoded.

# The following PySpark code encodes top n ports using One Hot Encoding, where n is specified by the variable ```top_ports```

# In[33]:


for i in range(0, top_ports):
    # "Port" + Top_Ports_list[i]  is the name of each new feature created through One Hot Encoding
    Scanners_df3 = Scanners3_df.withColumn("Port" + Top_Ports_list[i], array_contains("Ports_Array", Top_Ports_list[i]))
    Scanners3_df = Scanners_df3


# In[34]:


# Scanners3_df.printSchema()


# # Exercise 3 (15 points)
# Complete the following code to use k-means to cluster the scanners using the following features
# - bucketing of 'packets' numerical feature
# - one-hot encoding of top k ports (k=50)

# # Part D: Clustering
# ## Specify Parameters for k Means Clustering
# ## We use the variable `cluster_num` for the number of clusters to be generated by k-means clustering.

# In[35]:


cluster_num = 100
seed = 123
km = KMeans(featuresCol="features", predictionCol="prediction").setK(cluster_num).setSeed(seed)
km.explainParams()


# In[36]:


input_features = ["Packets_B10"]
for i in range(0, top_ports):
    input_features.append( "Port"+Top_Ports_list[i] )


# In[37]:


# print(input_features)


# In[38]:


va = VectorAssembler().setInputCols(input_features).setOutputCol("features")


# In[39]:


data= va.transform(Scanners3_df)


# In[40]:


data.persist()


# In[41]:


kmModel=km.fit(data)


# In[42]:


kmModel


# In[43]:


predictions = kmModel.transform(data)


# In[44]:


predictions.persist()


# In[45]:


Cluster0_df=predictions.where(col("prediction")==0)


# In[46]:


# Cluster0_df.count()


# # The following code shows the sizes of the clusters generated.

# In[47]:


summary = kmModel.summary


# In[48]:


summary.clusterSizes


# # Exercise 4 (5 points)
# ## Complete the following code to find the Silhouette Score of the clustering result.

# In[49]:


evaluator = ClusteringEvaluator()
silhouette = evaluator.evaluate(predictions)


# In[50]:


print('Silhouette Score of the Clustering Result is ', silhouette)


# In[51]:


centers = kmModel.clusterCenters()


# In[52]:


centers[0]


# In[ ]:


# print("Cluster Centers:")
# i=0
# for center in centers:
#    print("Cluster ", str(i+1), center)
#    i = i+1


# In[53]:


len(input_features)


# In[54]:


centers[0]


# In[55]:


centers[0][50]


# # Part E Use Mirai Signature (External Label) to Evaluate/Interpret Clustering Results

# # Exercise 5 (10 points)
# ## Complete the following code to compute the percentage of Mirai Malwares in each cluster.

# In[56]:


# Define columns of the Pandas dataframe
column_list = ['cluster ID', 'size', 'mirai_ratio' ]
for feature in input_features:
    column_list.append(feature)
mirai_clusters_df = pd.DataFrame( columns = column_list )
threshold = 0.2
for i in range(0, top_ports):
    cluster_row = [ ]
    cluster_i = predictions.where(col('prediction')==i)
    cluster_i_size = cluster_i.count()
    cluster_i_mirai_count = cluster_i.where(col("mirai")).count()
    cluster_i_mirai_ratio = cluster_i_mirai_count/cluster_i_size
    if cluster_i_mirai_count > 0:
        print("Cluster ", i, "; Mirai Ratio:", cluster_i_mirai_ratio, "; Cluster Size: ", cluster_i_size)
    if cluster_i_mirai_ratio > threshold:
        cluster_row = [i, cluster_i_size, cluster_i_mirai_ratio]
        # Add the cluster center (average) value for each input feature for cluster i to cluster_row
        for j in range(0, len(input_features)):
            cluster_row.append(centers[i][j])
        mirai_clusters_df.loc[i]= cluster_row


# # Exercise 6 (5 points) 
# ## Complete the code below to save the Pandas dataframe in a CSV file in your directory.

# In[57]:


mirai_clusters_df.to_csv("/storage/home/hxw5245/MiniProj3/Bucketing10_Cluster_Mirai.csv")


# # Part F Comparing with Clustering Results Using Only OHE 
# ## The following code cells are similiar to those in MiniProject 2, which generates a k-means clusteringresult based only on One-Hot-Encoding of ports being scanned. Execute all of the code cells below, including new ones (not in MiniProject 2) that uses Mirai labels to interpret the clusters that contain a significant portion of Mirai signatures.

# In[58]:


input_features2 = [ ]
for i in range(0, top_ports ):
    input_features2.append( "Port"+Top_Ports_list[i] )


# In[59]:


# print(input_features2)


# In[60]:


va2 = VectorAssembler().setInputCols(input_features2).setOutputCol("features2")


# In[61]:


data2= va2.transform(Scanners3_df)


# In[62]:


data2.persist()


# In[63]:


km2 = KMeans(featuresCol="features2", predictionCol="prediction2").setK(cluster_num).setSeed(seed)
km2.explainParams()


# In[64]:


kmModel2=km2.fit(data2)


# In[65]:


kmModel2


# In[66]:


predictions2 = kmModel2.transform(data2)


# In[67]:


predictions2.persist()


# In[68]:


summary2 = kmModel2.summary


# In[69]:


summary2.clusterSizes


# In[70]:


centers2 = kmModel2.clusterCenters()


# In[71]:


evaluator2 = ClusteringEvaluator(featuresCol='features2', predictionCol='prediction2')
silhouette2 = evaluator2.evaluate(predictions2)


# In[72]:


print('Silhouette Score of the Clustering Result is ', silhouette2)


# In[73]:


input_features2


# In[74]:


# Define columns of the Pandas dataframe
column_list2 = ['cluster ID', 'size', 'mirai_ratio' ]
for feature in input_features2:
    column_list2.append(feature)
mirai_clusters2_df = pd.DataFrame( columns = column_list2 )
threshold = 0.2
for i in range(0, top_ports):
    cluster_i = predictions2.where(col('prediction2')==i)
    cluster_i_size = cluster_i.count()
    cluster_i_mirai_count = cluster_i.where(col('mirai')).count()
    cluster_i_mirai_ratio = cluster_i_mirai_count/cluster_i_size
    if cluster_i_mirai_count > 0:
        print("Cluster ", i, "; Mirai Ratio:", cluster_i_mirai_ratio, "; Cluster Size: ", cluster_i_size)
    if cluster_i_mirai_ratio > threshold:
        cluster_row2 = [i, cluster_i_size, cluster_i_mirai_ratio]
        for j in range(0, len(input_features2)):
            cluster_row2.append(centers2[i][j])
        mirai_clusters2_df.loc[i]= cluster_row2


# # Exercise 7 (5 points)
# Complete the code below to save the Pandas dataframe in a CSV file in your directory

# In[75]:


mirai_clusters2_df.to_csv("/storage/home/hxw5245/MiniProj3/OHE_Cluster_Mirai.csv")


# # Exercise 8 (10 points) 
# ## Select a cluster that has a high mirai ratio. Complete the code below to see the distribtion of scanner's country in a cluster.

# In[76]:


# This is an example.  You should modify it based on the cluster you want to investigate.
cluster = predictions2.where(col('prediction2')==12)


# In[77]:


cluster.groupBy("Country").count().orderBy("count", ascending=False).show()


# # Exercise 9 (15 points)
# Modify a copy of this Jupyter Notebook for clustering the big data (Day_2020_profile) using the cluster mode in ICDS. 
# You will need to modify the two output files so that they you can later compare the output files between the local mode and the cluster mode.
# - You need to submit 
# - .py file for running in the cluster mode
# - The log file that contains the run time information.
# - One output file (CSV) for clusters generated from Bucketing Packets and OHE whose percentage of scanners matching the Mirai signature is higher than the threshold.
# - One output file (CSV) for clusters generated from only OHE whose percentage of scanners matching the Mirai signature is higher than the threshold.

# # Exercise 10 (20 points) Submit a word file that answers the following two questions.
# - (a) Discuss the characteristics for clusters generated from each approach (in the cluster mode).
# - (b) Compare the characteristics for clusters formed from each approach.

# In[ ]:


ss.stop()

