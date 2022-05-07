#!/usr/bin/env python
# coding: utf-8

# # DS/CMPSC 410 Spring 2022
# # Lab 8 Partition, Benefits, and a Use Case in Tweets Analytics
# # Instructor: Professor John Yen
# # TA: Rupesh Prajapati 
# # LAs: Lily Jakielaszek and Cayla Shan Pun
# 
# ## The goals of Lab 8 is to be able to apply partitionBy and coalesce to pre-partition/merge data partitions in RDD and understand its costs and benefits.
# 
# ###  This lab uses the same data set as the one used in Lab5. Like Lab5, the task is to identify top hashtags from tweets related to Boston Marathon Bombing, and idenity users who tweets about the hashtag.
# ### In the local mode (for debugging and testing), we will random sample 1 percent of the data, find top 5 hashtags, and Twitter users who send more than one tweet for each of these hashtags.
# ### In the cluster mode, you will use the entire dataset, find top 40 hashtags, and Twitter users who send more than three tweets for each of these top hashtags.
# 
# ### Applications of This Use Case:
# #### One of the tweets analytics opportunity is to uncover factors that affect the popularity (growth) of a topic, an opinion, or a hashtag.  One of the useful information for these tweets analytics is the number of tweets a user sent on a topic, an opinion, or a tweet, which can be used as a feature for a model to predict social influence or as a basis to create a network for graph analytics.
# 
# ## Comparison of impact of coalesce on performance:
# ### In order to compare the impact of using coalesce vs not using coalesce on the computational time of this Spark tweet analytics application, you will create TWO .py files: Lab8_baseline.py and Lab8_c?.py (where "?" is the parameter of coalesce).  The file Lab8_base.py contains the code that has been properly modifed for running in the cluster (see "Steps to do BEFORE exporting this Notebook to .py file" at the end of this Notebook). This will generate a baseline run-time performance, which you will use to compare with the performance of using coalesce on hashtag_count_rdd. 
# 
# ## The choice of number of partitions `hashtags_count_rdd` is coalesced into:
# ### Your goal is to reduce the cost of shuffling (during sort) by coalescing `hashtags_count_rdd` into a smaller number of partitions. Even though we have a total of 20 cores in the cluster mode, I recommend that you consider a smaller number (e.g., a number less than 10) as the parameter of coalesce.
# 
# ### Note:
# #### After you copy Lab8_baseline.py to Lab8_c?.py, make sure you not only add the .coalesce statements, but also make the following modifications:
# - Replace the name of RDD hashtag_count_rdd referred later in the code with the name of the RDD generated by the coalesce operation. 
# - Modify the output path of top_hashtags
# - Modify the output path of users that tweet top hashtags.
# 
# ## Submit the following items for Lab 8
# - Completed Jupyter Notebook of Lab 8
# - Lab8_baseline.py (used for spark-submit to obtain baseline)
# - Lab8_c?.py (where ? is the number you use in coalesce)
# - Log file for Lab8_baseline.py (e.g., Lab8_baseline_log.txt)
# - Log file for Lab8_c?.py (e.g., Lab8_c?_log.txt where ? is the number you used in coalesce)
# - Screen shot 
# - The first file (part-00000) in top_hashtags_baseline.txt (generated in cluster mode using Lab8_baseline)
# - A file of top Twitter Users in a top hashtag of your choice.
# - A word file that compares the run-time performance of the baseline code and the code with coalesce, and provide an explanation regarding cost and benefits of the coalesce operation.
# 
# ## Total Number of Exercises: 8
# - Exercise 1: Name and path for input file: 5 points
# - Exercise 2: Sort on non-key: 10 points
# - Exercise 3: 10 points (output top hashtag in local mode)
# - Exercise 4: 10 points (output users who send top hashtags in local mode)
# - Exercise 5: Successful execution and `time` information regarding  spark-submit in ICDS cluster for Lab8_baseline.py: 20 points
# - Exercise 6: Successful execution and `time` information regarding spark-submit in ICDS cluster for Lab8_c?.py (? refers to the number of partitions being coalesced into): 20 points
# - Exercise 7: Output files for cluster mode: 10 points
# - Exercise 8: Word file comparing the run-time performance of baseline code with the performance of the code with coalesce, and discuss the performance difference: 15  points
# ## Total Points: 100 points
# 
# # Due: midnight, March 6, 2022

# ## The first thing we need to do in each Jupyter Notebook running pyspark is to import pyspark first.

# In[1]:


import pyspark


# ## This lab will use both DataFrame and RDD. 

# In[2]:


from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, LongType, IntegerType, FloatType
from pyspark.sql.functions import col, column
from pyspark.sql.functions import expr
from pyspark.sql.functions import split
from pyspark.sql import Row
from pyspark.sql.functions import array_contains


# ## We then create a Spark Context variable.  Once we have a spark context variable, we can execute spark codes.

# In[3]:


ss=SparkSession.builder.appName("Lab 8 Partition Boston Marathon").getOrCreate()


# ## Exercise 5.1 (5 points) 
# (a) Add your name below.  
# (b) Replace the path below with the path of the path of your input file.
# ## Answer for Exercise 1
# (a) Your Name: Haichen Wei

# In[4]:


BMB_schema = StructType([ StructField("TweetID", LongType(), True ),                           StructField("UserID", StringType(), True),                           StructField("Time", StringType(), True ),                           StructField("Text", StringType(), True)                          ])


# In[5]:


BMB_DF = ss.read.csv("/storage/home/hxw5245/Lab8Partition/0417.csv", schema=BMB_schema, header=False, inferSchema=False)


# # Part 2 Calculate the count of all hashtags in the Twitter dataset

# ### The following code parses the Text column of the dataframe into tokens. It uses `split` SQL function introduced in Lab 4.  The delimiter for separating tokens (i.e., words, hashtags, twitter users) is the space character `' '`. 

# In[9]:


BMB2_DF = BMB_DF.withColumn("Tokens", split(col("Text"), ' '))



# ## Calculate the count of hashtags in 4 steps
# - Select the `Tokens` column from the RDD, convert the selected single-column DF into an RDD.
# - Use flatMap to convert the RDD of token list into an RDD of tokens.
# - Use filter to filter for hashtags from the token RDD.
# - Use map and reduceByKey to calculate the count for each hashtags

# In[11]:


Token_list_rdd = BMB2_DF.select("Tokens").rdd



# In[13]:


token_rdd = Token_list_rdd.flatMap(lambda row: (row.Tokens))



# In[15]:


hashtag_rdd = token_rdd.filter(lambda x: x.startswith("#"))



# In[17]:


hashtag_count_rdd = hashtag_rdd.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x+y)


# In[18]:


hashtag_count_rdd




# # Part 3 Find Top hashtags 

# # Exercise 2 (10 points)
# ## Complete the following code for sorting on the count of hashtags.

# In[20]:


sorted_hashtag_count_rdd = hashtag_count_rdd.sortBy(lambda x: x[1], ascending=False)




# In[22]:


top_hashtags_rdd = sorted_hashtag_count_rdd.filter(lambda x: x[1] > 100)


# # Exercise 3 (10 points)
# ## Edit the path with the correct path for storing your top hashtags.

# In[23]:


top_hashtags_rdd.saveAsTextFile("/storage/home/hxw5245/Lab8Partition/top_hashtags_cluster1.txt")


# # We want to select top k tweets based on their count.
# - In the local mode, set top_k to 5
# - In the cluster mode, set top_k to 40

# In[24]:


top_k = 40
# top_hashtag_list = sorted_hashtag_count_rdd.map(lambda x: x[0]).take(top_k)


# In[25]:


top_hashtag_list


# # Exercise 4 (10 points)
# ## Complete the code below by (1) replacing ? with the threshold (1 for local mode, 3 for cluster mode), (2) editing the output path to the correct output path for your output for users who tweet top hashtags.

# In[26]:


# Initialize the index to the top_hashtag_list
index = 0

BMB3_DF = BMB2_DF.select("UserID", "Tokens")
BMB3_DF.persist()

# Iterate the loop through each top k hashtag 
for i in range(0, top_k ):
    # For each top tweet, do the following:
    # 1. Filter the PySpark BMB2_DF DataFrame for those tweets that contain the top tweet.
    # 2. Group the filtered dataframe by UserID
    # 3. Count the number of tweets in each group (each UserID), which is the frequency the user tweets the hashtag.
    top_hashtag_DF = BMB3_DF.filter(array_contains(col("Tokens"), top_hashtag_list[i]))
    user_count_DF = top_hashtag_DF.groupBy(col("UserID")).count()
    filtered_user_count_DF = user_count_DF.where(col("count") > 3 )
    filename = "/storage/home/hxw5245/Lab8Partition/0417_UserID_cluster1" + top_hashtag_list[i]
    filtered_user_count_DF.write.csv(filename)                                 


# In[27]:


ss.stop()


# # Steps to do BEFORE exporting this Notebook to .py file
# - Make a copy of this Jupyter Notebook (e.g., Lab8_cluster.ipynb)
# - Remove .master("local") in the SparkSession statement in Lab8_cluster.ipynb
# - Comment out all statements of printSchema, show, and take.
# - Comment out (or remove) Part 1 in Lab8_cluster.ipynb (so that the code works on the entire dataset)
# - Change the value of top_k to 40
# - Change the value of the threshold for generating filtered_user_count_DF to 3.
# - Change the output directory for top_hash_tag (e.g., top_hashtag_cluster_1)
# - Change the output directory for Users of top 40 hashtags to a different prefix (e.g., "0417_UserID_cluster1")

# # Exercise 5 (20 points)
# ## Successful execution and `time` information regarding spark-submit in ICDS cluster for Lab_baseline.py

# # Exercise 6 (20 points)
# ## Successful execution and `time` information regarding spark-submit in ICDS cluster for Lab8_c?.py (? refers to the number of partitions being coalseced into).

# # Exercise 7 (10 points)
# ## Output Files (top hashtags, users involved in a top hashtag of your choice) for cluster mode

# # Exercise 8 (15 points)
# ## A word file comparing the run-time performance of baseline code with the performance of the code with coalesce, and explain the reason of the performance difference.

# In[ ]:




