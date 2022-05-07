#!/usr/bin/env python
# coding: utf-8

# ## DS/CMPSC 410 Spring 2022
# ## Instructor: Professor John Yen
# ## TA: Rupesh Prajapati 
# ## LAs: Lily Jakielaszek and Cayla Shan Pun
# ## Lab 3: Hashtag Twitter Counts, Cluster Mode, and Persist
# ## The goals of this lab are for you to be able to
# ## - Use SparkSession (of Spark SQL) to read csv file into a DataFrame
# ## - Use withColumn of DataFrame and SQL function split to create a new column for parking tweets into tokens.
# ## - Be familiar with running pyspark in ICDS cluster 
# ## - Evaluate the impact of using persist on an RDD used multiple times
# ## - Be familiar with the following RDD transformations in Spark:
# `filter, sortByKey'
# ## - Apply the obove to find top hashtags and top Twitters mentioned in a set of tweets.
# 
# ## Total Number of Exercises: 6
# - Exercise 1: 5 points
# - Exercise 2: 5 points
# - Exercise 3: 10 points
# - Exercise 4: 10 points
# - Exercise 5: 10 points
# - Exercise 6: 15 points
# 
# ## Total Points: 55 points
# 
# # Due: midnight, Jan 30th, 2022

# ## Like Lab 2, the first thing we need to do in each Jupyter Notebook running pyspark is to import pyspark first.

# In[1]:


import pyspark


# ## In addition to `SparkContext`, we import `SparkSession` from Spark SQL module. This enables us to read a CSV file into a structured Spark representation (called DataFrame).
# ## We also need to import two SQL functions: `split` and `col`
# ## Finally, we import Row from Spark SQL so that we can convert DataFrame into RDD.

# In[2]:


from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import split,col,column
from pyspark.sql import Row


# ## We then create a Spark Session variable (rathern than Spark Context) in order to use DataFrame.  Once we have a spark session variable, we can use spark codes associated with Spark SQL module (e.g., using DataFrame).
# 
# - Note: We temporarily use "local" as the parameter for master in this notebook so that we can test it in ICDS Jupyter Server.  However, we need to remove `.master("local")` later when you convert this notebook into a .py file to run in cluster mode.

# In[3]:


ss=SparkSession.builder.appName("Lab3").getOrCreate()
ss


# # Exercise 1 (5 points)  Add your name below 
# ## Answer for Exercise 1
# - Your Name: Haichen Wei

# # Schema of Data Frame:
# Each DataFrame needs a `schema`, which defines the heading and type of each column. The following code uses existing heading in the input csv file and uses the content of CSV file to infer the schema.  We will talk about how to explicitly define a schema next week.

# # Exercise 2 (5 points) 
# ## Complete the code below using your home directory to read the CSV file that contains tweets and sentiment label (one line for each tweet) for later processing.

# In[5]:


tweets_DF = ss.read.csv("/storage/home/hxw5245/Lab2/TweetsClimateChangeSentiment(1).csv", header=True, inferSchema=True)
tweets_DF




# # Exercise 3 (10 points) 
# ## Execute the code below, which computes total counts of hashtags and Twitter users in the given dataset, sort them by count (in descending order), and save them in two output directories:
# - (a) Uses an SQL split function to parse a tweet into a list of tokens.
# - (b) Filter the token into two lists: a list of hashtags and a list of twitter users.
# - (c) Count the total number of hashtags and total number of twitter users.
# - (d) Sort both the hashtag count and Twitter user count in descending order.
# - (e) Save the sorted hashtag counts and sorted Twitter user counts in two output directories.

# ## Code for Exercise 3 is shown in the Code Cells below.

# ## Adding a Column to a DataFrame using withColumn
# ### We often need to transform content of a column into another column. For example, we would like to transform the column Text in the tweets DataFrame into a `list` of tokens. We can do this using the DataFrame method `withColumn`.  
# 
# ### This transformation can be done using `split` Spark SQL function (which is different from python `split` function).
# ### The `split` Spark SQL function takes two arguments:
# - The first argument refers to a column of the DataFrame, such as `col("Text")`.
# - The second argument is a string that serves as the delimeter. 

# In[8]:


tweets_tokens_DF= tweets_DF.withColumn("Tokens", split(col("Text"), " "))


# # View Content of a DataFrame
# ### Like RDD, DataFrame is not actually computed until an action is invoked.  To see some content of the RDD, we can use `.show(n)` where n is the number of rows we want to show.  Like `.take` for RDD, `.show` for DataFrame is an action.



# ## DataFrame Transformation for Selecting Columns
# 
# ### DataFrame transformation `select` is similar to the projection operation in SQL: it returns a DataFrame that contains all of the columns selected.

# ## Conversion from a DataFrame to RDD
# ### A DataFrame can be converted to an RDD by the method `.rdd`

# In[10]:


token_list_RDD = tweets_tokens_DF.select("Tokens").rdd




# # Row of DataFrame
# ## When a DataFrame is converted into an RDD, it maintains the strucutre of the DataFrame (e.g., heading) using a `Row` structure, as shown in the output in the previous code cell. To access a specific column of a row, we can use the column name.  For example, the code below use `row.Tokens` to access the value of Tokens column for each row in the RDD (converted from the DataFrame tweets_token_DF).

# In[12]:


token_list2_RDD= token_list_RDD.map(lambda row: row.Tokens)



# In[14]:


token_RDD = token_list2_RDD.flatMap(lambda x: x)



# # Filtering an RDD
# 
# ## The syntax for filter (one type of data trasnformation in spark) is
# ## RDD.filter(lambda parameter : condition ) ##
# ## Notice the syntax is not what is described in p. 38 of the textbook.
# ##  The result of filtering the input RDD is the collection of all elements that pass the filter condition (i.e., returns True when the filtering condition is applied to the parameter. 
# ## For example, the filtering condition in the pyspark conde below checks whether each element of the input RDD (i.e., token_RDD) starts with the character "#", using Python startswith() method for string.

# In[16]:


hashtag_RDD = token_RDD.filter(lambda pair: pair[0].startswith("#"))


# In[17]:


twitter_RDD = token_RDD.filter(lambda pair: pair[0].startswith("@"))


# In[18]:


hashtag_1_RDD = hashtag_RDD.map(lambda x: (x, 1))


# In[19]:


twitter_1_RDD = twitter_RDD.map(lambda x: (x, 1))


# In[20]:


hashtag_count_RDD = hashtag_1_RDD.reduceByKey(lambda x, y: x+y, 3)


# In[21]:


twitter_count_RDD = twitter_1_RDD.reduceByKey(lambda x, y: x+y, 3)


# ## To sort hashtag and Twitter user count so that those that occur more frequent appear first, we switch the order of key and value (so that key is count, value is hashtag or Twitter user) using map.  
# ## `sortByKey` sort a key value RDD based on the order of key.  The default order is ascending, so we need to set the flag `ascending` to `False` so that more frequently occured hashtags and Twitter users occur first.

# In[22]:


sorted_count_hashtag_RDD = hashtag_count_RDD.map(lambda x: (x[1], x[0])).sortByKey(ascending=False)


# In[23]:


sorted_count_twitter_RDD = twitter_count_RDD.map(lambda x: (x[1], x[0])).sortByKey(ascending=False)



# # Note: You need to replace the path with your directory. You also need to change the directory names (e.g., replace "1" with "2" or "B") before you convert this notebook into a .py file for submitting it to ICDS.  

# In[24]:


output_hashtag = "/storage/home/hxw5245/Lab3/sorted_count_hashtag2" 
sorted_count_hashtag_RDD.saveAsTextFile(output_hashtag)


# In[25]:


output_twitter = "/storage/home/hxw5245/Lab3/sorted_count_twitter2" 
sorted_count_twitter_RDD.saveAsTextFile(output_twitter)


# In[26]:


ss.stop()


# # Exercise 4 (10 points)
# After running this Jupyter Notebook successfully in local mode, modify the notebook in two ways
# - remove `.master("local")` in SparkSession statement so that the Spark code runs in cluster (Stand Alone cluster) mode.
# - modify the output directory names
# - Export the notebook as Executable Script (change the file name to Lab3.py)
# - upload Lab3.py to this directory.
# - Follow the direction of "Instructions for Running Spark in Cluster Mode"

# # Exercise 5 (10 points)
# Add persist() to token_RDD in your Lab3.py file.  You can edit the Lab3.py file directly in Jupyter Server (this window), then use `Save Python File as` pull down menu under `File` on the upper left corner of this window to save it `Lab3B.py`. When you run spark-submit, save the log output file in a filae DIFFERENT from that you used for Exercise 4 (e.g., Lab3_log.txt for Exercise 4, Lab3B_log.txt for Exercise 5).

# # Exercise 6 (15 points)
# Compare the run-time cost (both real time and user time) for running in cluster mode with persist and without persist. Provide your answer below:

# # Answer to Exercise 6:
# - Type your answer here .
