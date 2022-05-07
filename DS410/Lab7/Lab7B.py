#!/usr/bin/env python
# coding: utf-8

# # DS/CMPSC 410 Spring 2022
# # Lab 7 Decision Tree Learning Using ML Pipeline, Visualization, and Hyperparameter Tuning
# 
# # Instructor: Professor John Yen
# # TA: Rupesh Prajapati 
# # LAs: Lily Jakielaszek and Cayla Shan Pun
# 
# ## The goals of this lab are for you to be able to
# - Understand the function of the different steps/stages involved in Spark ML pipeline
# - Be able to construct a decision tree using Spark ML machine learning module
# - Be able to generate a visualization of Decision Trees
# - Be able to use Spark ML pipeline to perform automated hyper-parameter tuning for Decision Trees 
# - Be able to apply .persist() to suitable DataFrames and evaluate its impact on computation time.
# 
# ## The data set used in this lab is a Breast Cancer diagnosis dataset.
# 
# ## Submit the following items for Lab 7 (DT)
# - Completed Jupyter Notebook of Lab 7 (in HTML format)
# - A .py file (e.g., Lab7DT.py) and logfile for running ONLY part 5 of this in cluster 
# - A .py file (e.g., Lab7DT_P.py) and logfile for running ONLY part 5 of this in cluster, with persist() on two DataFrames.
# - The output file that contains the best hyperparameters
# 
# ## Total Number of Exercises: 100
# - Exercise 1: 5 points
# - Exercise 2: 5 points
# - Exercise 3: 10 points  
# - Exercise 4: 10 points 
# - Exercise 5: 15 points
# - Exercise 6: 15 points
# - Exercise 7: 20 points
# - Exercise 8: 20 points
# ## Total Points: 100 points
# 
# # Due: midnight, Feb 27, 2022

# # Load and set up the Python files for this Lab
# 1. Create a "Lab7DT" directory in the work directory of your ICDS-ROAR home directory.
# 2. If you have not done so, copy or upload this file to the "Lab7DT" directory.
# 3. Create a subdirectory under "Lab7DT" called "decision_tree_plot" (named the directory EXACTLY this way).
# 4. Upload the following three files in Module 8 from Canvas to the decision_tree_plot directory
# - decision_tree_parser.py
# - decision_tree_plot.py
# - tree_template.jinjia2
# 
# # Follow the instructions below and execute the PySpark code cell by cell below. Make modifications as required.

# In[1]:


import pyspark
import pandas as pd
import csv


# ## Notice that we use PySpark SQL module to import SparkSession because ML works with SparkSession
# ## Notice also the different methods imported from ML and three submodules of ML: classification, feature, and evaluation.

# In[2]:


from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, LongType, IntegerType, FloatType
import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.ml import Pipeline
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler, IndexToString
from pyspark.ml.evaluation import MulticlassClassificationEvaluator, BinaryClassificationEvaluator
from pyspark.sql.functions import col

# ## The following two lines import relevant functions from the two python files you uploaded into the decision_tree_plot subdirectory.

# In[3]:


from decision_tree_plot.decision_tree_parser import decision_tree_parse
from decision_tree_plot.decision_tree_plot import plot_trees


# ## This lab runs Spark in the local mode.
# ## Notice we are creating a SparkSession, not a SparkContext, when we use ML pipeline.
# ## The "getOrCreate()" method means we can re-evaluate this without a need to "stop the current SparkSession" first (unlike SparkContext).

# In[4]:


ss=SparkSession.builder.appName("lab 7 DT").getOrCreate()


# ## Exercise 1: (5 points) Enter your name below:
# - My Name: Haichen Wei

# ## As we have seen in Lab 4, SparkSession offers a way to read a CSV/text file with the capability to interpret the first row as being the header and infer the type of different columns based on their values.

# ## Exercise 2: (5 points) Complete the following path with the path for your home directory.  

# In[6]:


data = ss.read.csv("/storage/home/hxw5245/Lab7DT/breast-cancer-wisconsin.data.txt", header=True, inferSchema=True)





# In[10]:


bnIndexer = StringIndexer(inputCol="bare_nuclei", outputCol="bare_nuclei_index").fit(data)

labelIndexer= StringIndexer(inputCol="class", outputCol="indexedLabel").fit(data)

input_features = ['clump_thickness', 'unif_cell_size', 'unif_cell_shape', 'marg_adhesion',                   'single_epith_cell_size', 'bare_nuclei_index', 'bland_chrom', 'norm_nucleoli', 'mitoses']




# # Part 5 Automated Hyperparameter Tuning for Decision Tree

# ## Exercise 5: (15 points)  
# - Complete the code below to perform hyper parameter tuning of Decision Tree (for two parameters: max_depth and minInstancesPerNode)

# In[ ]:


trainingData, testingData= data.randomSplit([0.75, 0.25], seed=1234)
model_path="./DTmodel_vis"

trainingData.persist()
testingData.persist()

## Initialize a Pandas DataFrame to store evaluation results of all combination of hyper-parameter settings
hyperparams_eval_df = pd.DataFrame( columns = ['max_depth', 'minInstancesPerNode', 'training f1', 'testing f1', 'Best Model'] )
# initialize index to the hyperparam_eval_df to 0
index =0 
# initialize lowest_error
highest_testing_f1 = 0
# Set up the possible hyperparameter values to be evaluated
max_depth_list = [2, 3, 4, 5, 6, 7, 8, 9, 10, 11]
minInstancesPerNode_list = [2, 3, 4, 5, 6]
assembler = VectorAssembler( inputCols=input_features, outputCol="features")
labelConverter = IndexToString(inputCol = "prediction", outputCol="predictedClass", labels=labelIndexer.labels)
for max_depth in max_depth_list:
    for minInsPN in minInstancesPerNode_list:
        seed = 37
        # Construct a DT model using a set of hyper-parameter values and training data
        dt= DecisionTreeClassifier(labelCol="indexedLabel", featuresCol="features", maxDepth= max_depth, minInstancesPerNode= minInsPN)
        pipeline = Pipeline(stages=[labelIndexer, bnIndexer, assembler, dt, labelConverter])
        model = pipeline.fit(trainingData)
        training_predictions = model.transform(trainingData)
        testing_predictions = model.transform(testingData)
        evaluator = MulticlassClassificationEvaluator(labelCol="indexedLabel", predictionCol="prediction", metricName="f1")
        training_f1 = evaluator.evaluate(training_predictions)
        testing_f1 = evaluator.evaluate(testing_predictions)
        # We use 0 as default value of the 'Best Model' column in the Pandas DataFrame.
        # The best model will have a value 1000
        hyperparams_eval_df.loc[index] = [ max_depth, minInsPN, training_f1, testing_f1, 0]  
        index = index +1
        if testing_f1 > highest_testing_f1 :
            best_max_depth = max_depth
            best_minInsPN = minInsPN
            best_index = index -1
            best_parameters_training_f1 = training_f1
            best_DTmodel= model.stages[3]
            best_tree = decision_tree_parse(best_DTmodel, ss, model_path)
            column = dict( [ (str(idx), i) for idx, i in enumerate(input_features) ])           
            highest_testing_f1 = testing_f1 

# column = dict([(str(idx), i) for idx, i in enumerate(input_features)])
plot_trees(best_tree, column = column, output_path = '/storage/home/hxw5245/Lab7DT/bestDTtree2.html')


# In[ ]:


# Store the Testing RMS in the DataFrame
hyperparams_eval_df.loc[best_index]=[best_max_depth, best_minInsPN, best_parameters_training_f1, highest_testing_f1, 1000]


# In[ ]:


schema3= StructType([ StructField("Max Depth", FloatType(), True),                       StructField("MinInstancesPerNode", FloatType(), True ),                       StructField("Training f1", FloatType(), True),                       StructField("Testing f1", FloatType(), True),                       StructField("Best Model", FloatType(), True)                     ])


# ## Convert the pandas DataFrame that stores validation errors of all hyperparameters and the testing error for the best model to Spark DataFrame
# 

# In[ ]:


HyperParams_Tuning_DF = ss.createDataFrame(hyperparams_eval_df, schema3)


# ## Exercise 6 (15 points)
# ### Complete the path below to save the result of your hyperparameter tuning in a directory.
# 
# ## Notice: Modify the output path before you export this to a .py file for running in cluster mode.  
# ## Notice: Remember to change the output_path directory after each spark-submit in cluster. Otherwise, the spark_submit will NOT run the action (saveAsTextFile) successfully due to being unable to write into an existing directory.

# In[ ]:


output_path = "/storage/home/hxw5245/Lab7DT/Lab7_B"
HyperParams_Tuning_DF.rdd.saveAsTextFile(output_path)


# In[ ]:


ss.stop()


# # Exercise 7 (20 points) 
# ## Modify this Notebook to comment out (or remove) Part 1, 2, 3, and 4, and modify it for running spark-submit in cluster mode.  Export it as a .py file (e.g., Lab7DT.py). Record the computation time below:

# ## Answer to Exercise 7:

# # Exercise 8 (20 points)
# ## Modify .py file used in Exercise 7 to add `persist()` to two DataFrame for enhanced scalability of the code. (a) Record the computation time below. (b) Compare the computation time with and without persist.

# ## Answer to Exercise 8: 

# In[ ]:




