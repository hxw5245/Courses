# data wrangling
rm(list=ls())

# load the packages
library(data.table)

# load in data 
train_data <-fread("./project/volume/data/raw/train_data.csv")
test_file <- fread("./project/volume/data/raw/test_file.csv")

drops <- c('text')
train_data <- train_data[, !drops, with = FALSE]
test <- test_file[, !drops, with = FALSE]

#transform data into xgboost format
train <-melt(train_data,id=c("id"),variable.name = "category")
train <-train[value==1][,.(id,category)]
train$category <- as.integer(train$category)-1

train <- merge(train_data, train, sort=FALSE)
train <- train[,.(id, category)]

test$category<-0

# save the data
fwrite(train,"./project/volume/data/interim/train.csv")
fwrite(test,"./project/volume/data/interim/test.csv")
