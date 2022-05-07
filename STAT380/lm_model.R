rm(list = ls())
library(caret)
library(data.table)
library(Metrics)

setwd("/Users/Zoey/Desktop/house_price/project")

#load the data
train <- fread('volume/data/raw/Stat_380_train.csv')
test <- fread('volume/data/raw/Stat_380_test.csv')

avg <- mean(train$SalePrice, na.rm = T)

test$SalePrice <- 0
train_y <- train$SalePrice

dummies <- dummyVars(SalePrice ~ ., data = train)

train <- predict(dummies, newdata = train)
train <- data.table(train)
train$SalePrice <- train_y

test <- predict(dummies, newdata = test)
test <- data.table(test)

#fit a linear model
lm_model<-lm(SalePrice ~ .,data=train)

#assess model
summary(lm_model)

#save model
saveRDS(dummies,"volume/models/SalePrice_lm.dummies")
saveRDS(lm_model,"volume/models/SalePrice_lm.model")

# prediction
test$SalePrice<-predict(lm_model,newdata = test)

#submission
submit<-test[,.(Id, SalePrice)]
submit[is.na(submit$SalePrice)]$SalePrice <- avg
fwrite(submit,"volume/data/processed/submit_lm.csv")
