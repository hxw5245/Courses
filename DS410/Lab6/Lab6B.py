#!/usr/bin/env python
# coding: utf-8

# # DS/CMPSC 410 Spring 2022
# # Instructor: Professor John Yen
# # TA: Rupesh Prajapati 
# # LAs: Lily Jakielaszek and Cayla Shan Pun
# # Lab 6: Big Data Movie Recommendations Using Spark-submit, Persist and Looping
# 
# ## The goals of this lab, building on Lab 6, are for you to be able to
# ### - Define schema for reading a big movie rating file
# ### - Understand a potential flaw for random sampling from Big Data.
# ### - Sample from the big rating data in a more suitable way.
# ### - Design different options for persist RDD/DF involved in the iterative cycle for hyper parameter tuning.
# ### - Evaluate the run-time performance of different options of persist using the Big review data.
# ### - Manage jobs in ICDS Roar 
# 
# ## Exercises: 
# - Exercise 1: 5 points
# - Exercise 2: 5 points (schema definition)
# - Exercise 3: 10 points (total number of users and movies)
# - Exercise 4: 15 points (Evaluation of ALS model from random sampling)
# - Exercise 6: 10 points (Evaluation of ALS model from systematic sampling)
# - Exercise 6: 10 points (Changes to Lab7B for spark-submit to cluster)
# - Exercise 7: 15 points (Word file: Rationale of the choice of three persist.)
# - Exercise 8: 30 points (Word file: Performance of the three options of persist design.)
# ## Total Points: 100 points
# 
# # Due: midnight, February 20, 2022

# ## The first thing we need to do in each Jupyter Notebook running pyspark is to import pyspark first.

# In[1]:


import pyspark
import pandas as pd
import numpy as np
import math


# ### Once we import pyspark, we need to import "SparkContext".  Every spark program needs a SparkContext object
# ### In order to use Spark SQL on DataFrames, we also need to import SparkSession from PySpark.SQL

# In[2]:


from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, LongType, IntegerType, FloatType
from pyspark.sql.functions import col, column
from pyspark.sql.functions import expr
from pyspark.sql.functions import split
from pyspark.sql import Row
from pyspark.mllib.recommendation import ALS


# ## We then create a Spark Session variable (rather than Spark Context) in order to use DataFrame. 
# - Note: We temporarily use "local" as the parameter for master in this notebook so that we can test it in ICDS Roar.  However, we need to remove ".master("local")" before we submit it to ICDS to run in the cluster mode.

# In[3]:


ss=SparkSession.builder.appName("Lab6A Recommendation Using Big Data").getOrCreate()


# In[4]:


ss.sparkContext.setCheckpointDir("~/scratch")


# # Exercise 1 (5 points) 
# ## Add your name below.
# ## Answer for Exercise 1:
# - Your Name: Haichen Wei

# # Exercise 2 (5 points) 
# ## Complete the schema definition and run the code cells below so that rating_schema can be used to read the big movie rating file (csv format) into a PySpark DataFrame

# In[5]:


ratingsbig_DF = ss.read.csv("/storage/home/hxw5245/Lab6/ratings-large.csv", header=True, inferSchema=True)


# In[6]:


# ratingsbig_DF.printSchema()


# In[7]:


rating_schema = StructType([ StructField("UserID", IntegerType(), False ),                             StructField("MovieID", IntegerType(), True),                             StructField("Rating", FloatType(), True ),                             StructField("RatingID", IntegerType(), True ),                            ])


# In[8]:


ratings_DF = ss.read.csv("/storage/home/hxw5245/Lab6/ratings-large.csv", schema= rating_schema, header=False, inferSchema=False)


# In[9]:


# ratings_DF.printSchema()

"""
# # 6.1 Investigating Size of the Big Review Data

# # Exercise 3 (10 points)
# - (a) What is the number of users in this big movie rating dataset? (5 points)
# - (b) What is the number of movies in this big movie rating dataset? (5 points)
# ## After running the code cells below, enter your answer in the Markdown cell below for Answer to Exercise 3.

# In[10]:


users_DF = ratings_DF.select("UserID")


# In[11]:


# users_DF.show(3)


# ## We use `.dropDuplicates()` to remove duplicate rows of a PySpark DataFrame. The following code calculate the number of unique users in the large ratings dataset.

# In[12]:


UnqUsr_DF = users_DF.dropDuplicates()


# In[13]:


UserCnt= UnqUsr_DF.count()


# In[14]:


movies_DF = ratings_DF.select("MovieID")


# In[15]:


UnqMovie_DF = movies_DF.dropDuplicates()


# In[16]:


MovieCnt = UnqMovie_DF.count()


# In[17]:


# print("User Count =", UserCnt, "Movie Count =", MovieCnt)


# # Answer to Exercise 3
# - (a) The number of users is 259137.
# - (b) The number of movies is 39443.

# # 6.2. Random Sampling from Big Data
# ## We noticed the size of the users in the large dataset is about 400 times larger than that of the small dataset. We also noticed that the size of the movies in the large dataset is about 4 times larger than that of the small dataset. What problem will we run into if we randomly sample a small portion (e.g., 0.3%) from the large dataset?

# # Exercise 4 (15%) 
# ## Complete the code cell below to use a random sampled (0.3%) big movie review data to train an ALS model, then evaluate its RMS training error and RMS validation error.

# In[18]:


ratings2_DF = ratings_DF.sample(withReplacement=False, fraction=0.003, seed=19).select("UserID","MovieID","Rating")


# In[19]:


ratings2_RDD= ratings2_DF.rdd


# In[ ]:


# split ratings2_DF into training, validation, and testing


# In[20]:


training_RDD, validation_RDD, test_RDD = ratings2_RDD.randomSplit([3, 1, 1], 521)


# ## Prepare input (UserID, MovieID) for validation and for testing

# In[21]:


training_input_RDD = training_RDD.map(lambda x: (x[0], x[1]) )
validation_input_RDD = validation_RDD.map(lambda x: (x[0], x[1]) ) 
testing_input_RDD = test_RDD.map(lambda x: (x[0], x[1]) )


# In[22]:


model = ALS.train(training_RDD, 4, seed=41, iterations=30, lambda_=0.1)


# In[23]:


training_prediction_RDD = model.predictAll(training_input_RDD).map(lambda x: ( (x[0], x[1]), x[2] ) )


# In[24]:


# training_prediction_RDD.take(3)


# In[25]:


training_evaluation_RDD = training_RDD.map(lambda y: ( (y[0], y[1]), y[2]) ).join(training_prediction_RDD)


# In[26]:


# training_evaluation_RDD.take(3)


# In[27]:


training_error = math.sqrt(training_evaluation_RDD.map(lambda z: (z[1][0] - z[1][1])**2).mean())


# In[28]:


# print(training_error)


# In[29]:


validation_prediction_RDD = model.predictAll(validation_input_RDD).map(lambda x: ( (x[0], x[1]), x[2] ) )


# In[30]:


# validation_prediction_RDD.take(5)


# In[31]:


validation_evaluation_RDD = validation_RDD.map(lambda y: ( (y[0], y[1]), y[2] )).join(validation_prediction_RDD)


# In[32]:


# validation_evaluation_RDD.take(5)


# In[33]:


validation_error = math.sqrt(validation_evaluation_RDD.map(lambda z: (z[1][0] - z[1][1])**2).mean())


# In[34]:


# print(validation_error)


# # Finding 1: 
# The validation error is much bigger than the training error.  The validation error is also much larger than the validation error of Lab 5. What are possible reasons to explain this?
# - The space of users and movies is much larger than that of the Lab 5.
# - Because the much larger space of users and movies, the sampled ratings is a much sparser than the rating matrix of Lab 5.

# # What to do next?
# While we are primarily interested in creating a recommendation model using the entire big dataset (not just using a small sample), it is desirable to check that we can generate a reasonoable recommendation model (based on ALS) using a sampled dataset before we do this in the big dataset. Therefore, the next section discusses a sampling approach from the big dataset to reduce the sparsity of the rating matrix by (1) sampling a small group of users, and (2) using all reviews from this samll group of sampled users to create training, testing, and validation data.

# # 6.3. Sampling to Reduce the Sparsity of User and Movie Ratings Matrix
# ## To reduce the sparsity of the User X Movies rating matrix, we can sample a small subset of Users (e.g., 0.3 %) first, then join it with the big rating dataset to gather all reviews by these sampled users, whihc is used for creating training, testing, and validation data.
# ## Benefits of this sampling approach: 
# - The sparsity of the rating matrix is significantly reduced because the number of users is much smaller than that of the previous sampled dataset.
# - Such a small sampled rating matrix can be used to evaluate the feasibility of the ALS-based recommendation model for the dataset before we construct the model using the entire dataset.

# In[35]:


sampling_ratio = 0.003
sampled_User_DF = UnqUsr_DF.sample(withReplacement=False, fraction=sampling_ratio, seed=19)


# In[36]:


sampled_rating_DF = sampled_User_DF.join(ratings_DF, "UserID", "inner")


# In[37]:


sampled_User_DF.count()


# In[38]:


sampled_rating_DF.count()


# In[39]:


ratings2_DF = sampled_rating_DF.select("UserID","MovieID","Rating")


# In[40]:


ratings2_RDD = ratings2_DF.rdd


# # 6.3.2 Split Systematic Sampled Data into Training Data, Validation Data, and Testing Data

# In[41]:


training_RDD, validation_RDD, test_RDD = ratings2_RDD.randomSplit([3, 1, 1], 137)


# ### Split sampled data into training and validating data for constructing and evaluating an ALS model using a specific rank, iteration, and regularization parameter.  The purpose of this step is to check whether the performance of the sampled data from Big Data is comparable to that constructed from the small data (in Lab 6). 

# In[42]:


training_input_RDD = training_RDD.map(lambda x: (x[0], x[1]) )
validation_input_RDD = validation_RDD.map(lambda x: (x[0], x[1]) ) 
testing_input_RDD = test_RDD.map(lambda x: (x[0], x[1]) )


# In[43]:


model2 = ALS.train(training_RDD, 4, seed=37, iterations=30, lambda_=0.1)


# # 6.3.3 Using the constructed model to evaluate RMS of training data and testing data.

# # Exercise 5 (10%)
# ## Complete the code cells below for training and evaluating the recommendation model constructed from systematically sampled big review data.
# ## Note: Running some of the cells may take a while, due to the size of the data.  Make sure you do NOT proceed to run another cell while a cell is still running.

# In[44]:


training_prediction_RDD = model2.predictAll(training_input_RDD).map(lambda x: ( (x[0], x[1]), x[2] ) )


# In[45]:


# training_prediction_RDD.take(3)


# In[46]:


training_evaluation_RDD = training_RDD.map(lambda y: ( (y[0], y[1]), y[2]) ).join(training_prediction_RDD)


# In[47]:


# training_evaluation_RDD.take(3)


# In[48]:


training_error = math.sqrt(training_evaluation_RDD.map(lambda z: (z[1][0] - z[1][1])**2).mean())


# In[49]:


# print(training_error)


# In[51]:


validation_prediction_RDD = model2.predictAll(validation_input_RDD).map(lambda x: ( (x[0], x[1]), x[2] ) )


# In[52]:


# validation_prediction_RDD.take(5)


# In[53]:


validation_evaluation_RDD = validation_RDD.map(lambda y: ( (y[0], y[1]), y[2]) ).join(validation_prediction_RDD)


# In[54]:


# validation_evaluation_RDD.take(5)


# In[55]:


validation_error = math.sqrt(validation_evaluation_RDD.map(lambda z: (z[1][0] - z[1][1])**2).mean())


# In[56]:


# print(validation_error)


# # Finding 2: 
# The validation error of the second model, constructed using all reviews from a subset of users, is closer to the testing error than what we observed in Finding 1 (for the first model, constructed using a random sample from the big dataset).
"""

# # 6.4 Hyperparameter Tuning
# 
# ## Like Lab 5, you will need to iterate through all possible combination of a set of values for three hyperparameters for ALS Recommendation Model:
# - rank (k)
# - regularization
# - iterations 
# ## Each hyperparameter value combination is used to construct an ALS recommendation model using training data, but evaluate using Evaluation Data
# ## The evaluation results are saved in a Pandas DataFrame 
# ``
# hyperparams_eval_df
# ``
# ## The best hyperprameter value combination is stored in 4 variables
# ``
# best_k, best_regularization, best_iterations, and lowest_validation_error
# ``
# # However, you do NOT NEED TO execute the code below in Local mode, since you have tested the construction and evaluation of ALS model using a sampled data from the big review data.

# # Exercise 6 (10 points) 
# ## Duplicate this Jupyter Notebook (Lab6), name the new copy "Lab6A.ipynb". 
# - Make all modifications to Lab6A so that it is ready to be submitted to the cluster (but don't add any persist yet). 
# - In addition, comment out codes in Section 7.1, 7.2, and 7.3. Save the modified Lab6A notebook, and export it into Lab6A.py. 
# - Run spark-submit of Lab6A.py in ICDS cluster (following instructions of Lab 3) to generate the results of hyper-parameter tuning for the big dataset, as well as the time it takes (shown in the log file).

# In[ ]:


ratings4_DF = ratings_DF.select("UserID","MovieID","Rating")


# In[ ]:


ratings4_RDD = ratings4_DF.rdd


# In[ ]:


training_RDD, validation_RDD, test_RDD = ratings4_RDD.randomSplit([3, 1, 1], 137)


# In[ ]:


training_input_RDD = training_RDD.map(lambda x: (x[0], x[1]) )
validation_input_RDD = validation_RDD.map(lambda x: (x[0], x[1]) ) 
testing_input_RDD = test_RDD.map(lambda x: (x[0], x[1]) )

training_RDD.persist()
validation_RDD.persist()
validation_input_RDD.persist()

# In[ ]:


## Initialize a Pandas DataFrame to store evaluation results of all combination of hyper-parameter settings
hyperparams_eval_df = pd.DataFrame( columns = ['k', 'regularization', 'iterations', 'validation RMS', 'testing RMS'] )
# initialize index to the hyperparam_eval_df to 0
index =0 
# initialize lowest_error
lowest_validation_error = float('inf')
# Set up the possible hyperparameter values to be evaluated
iterations_list = [15, 30]
regularization_list = [0.1, 0.2]
rank_list = [4, 8, 12]
for k in rank_list:
    for regularization in regularization_list:
        for iterations in iterations_list:
            seed = 37
            # Construct a recommendation model using a set of hyper-parameter values and training data
            model = ALS.train(training_RDD, k, seed=seed, iterations=iterations, lambda_=regularization)
            # Evaluate the model using evalution data
            # map the output into ( (userID, movieID), rating ) so that we can join with actual evaluation data
            # using (userID, movieID) as keys.
            validation_prediction_RDD= model.predictAll(validation_input_RDD).map(lambda x: ( (x[0], x[1]), x[2] )   )
            validation_evaluation_RDD = validation_RDD.map(lambda y: ( (y[0], y[1]), y[2]) ).join(validation_prediction_RDD)
            # Calculate RMS error between the actual rating and predicted rating for (userID, movieID) pairs in validation dataset
            validation_error = math.sqrt(validation_evaluation_RDD.map(lambda z: (z[1][0] - z[1][1])**2).mean())
            # Save the error as a row in a pandas DataFrame
            hyperparams_eval_df.loc[index] = [k, regularization, iterations, validation_error, float('inf')]
            index = index + 1
            # Check whether the current error is the lowest
            if validation_error < lowest_validation_error:
                best_k = k
                best_regularization = regularization
                best_iterations = iterations
                best_index = index - 1
                lowest_validation_error = validation_error
# print('The best rank k is ', best_k, ', regularization = ', best_regularization, ', iterations = ',\
#      best_iterations, '. Validation Error =', lowest_validation_error)


# # Use Testing Data to Evaluate the Model built using the Best Hyperparameters                

# # 6.5 Evaluate the best hyperparameter combination using testing data

# In[ ]:


seed = 37
model = ALS.train(training_RDD, best_k, seed=seed, iterations=best_iterations, lambda_=best_regularization)
testing_prediction_RDD=model.predictAll(testing_input_RDD).map(lambda x: ((x[0], x[1]), x[2]))
testing_evaluation_RDD= test_RDD.map(lambda x: ((x[0], x[1]), x[2])).join(testing_prediction_RDD)
testing_error = math.sqrt(testing_evaluation_RDD.map(lambda x: (x[1][0]-x[1][1])**2).mean())
# print('The Testing Error for rank k =', best_k, ' regularization = ', best_regularization, ', iterations = ', \
#      best_iterations, ' is : ', testing_error)


# In[ ]:


# print(best_index)


# In[ ]:


# Store the Testing RMS in the DataFrame
hyperparams_eval_df.loc[best_index]=[best_k, best_regularization, best_iterations, lowest_validation_error, testing_error]


# In[ ]:


schema3= StructType([ StructField("k", FloatType(), True),                       StructField("regularization", FloatType(), True ),                       StructField("iterations", FloatType(), True),                       StructField("Validation RMS", FloatType(), True),                       StructField("Testing RMS", FloatType(), True)                     ])


# ## Convert the pandas DataFrame that stores validation errors of all hyperparameters and the testing error for the best model to Spark DataFrame
# 

# In[ ]:


HyperParams_Tuning_DF = ss.createDataFrame(hyperparams_eval_df, schema3)


# ## Modify the output path so that your output results can be saved in a directory.
# # Notice: Remember to change the output_path directory before each spark-submit in cluster. Otherwise, the spark_submit will NOT run the action (saveAsTextFile) successfully due to being unable to write into an existing directory.

# In[ ]:


output_path = "/storage/home/hxw5245/Lab6/Lab6_B"
HyperParams_Tuning_DF.rdd.saveAsTextFile(output_path)


# In[ ]:


ss.stop()


# # Exercise 7 (15%)
# ## Describe the rationale of three persists you added to the code in a word file (to be submitted for Lab 6)

# # Exercise 8 (30%)
# ## In the same word file, include two information for four versions of running Lab6B in the cluster: (a) the best hyperparameters, the validation error and the testing error, and (b) the run time (real, user, and sys) in ICDS-Roar using the configuration in the instruction for running spark-submit in ICDS-Roar.
# - 1. The baseline (no persist) Lab6A
# - 2. With three persists on RDDs: Lab6B

# In[ ]:




