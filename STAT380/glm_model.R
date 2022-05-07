rm(list = ls())

library(caret)
library(data.table)
library(Metrics)

setwd("/Users/Zoey/Desktop/coin_flip")

#load the data
train <- fread('./project/volume/data/raw/train_file.csv')
test <- fread('./project/volume/data/raw/test_file.csv')

#avg <- mean(train$result, na.rm = T)
test$result <- 0
train_y<-train$result

dummies <- dummyVars(result ~ ., data = train)
train<-predict(dummies, newdata = train)
test<-predict(dummies, newdata = test)

#reformat after dummyVars and add back response Var
train<-data.table(train)
train$result<-train_y
test<-data.table(test)

train <- train[, .(id, result, p_hat = (V1+V2+V3+V4+V5+V6+V7+V8+V9+V10)/10)]
test <- test[, .(id, p_hat = (V1+V2+V3+V4+V5+V6+V7+V8+V9+V10)/10)]

#fit the model
glm_model <- glm(result ~ p_hat, family=gaussian, data=train)


#assess model
summary(glm_model)
coef(glm_model)

#save model
saveRDS(dummies,"./project/volume/models/result.dummies")
saveRDS(glm_model,"./project/volume/models/result.model")

test$result<-predict(glm_model,newdata = test,type="response")

#submission
submit<-test[,.(id, result)]
submit$id <- as.integer(submit$id)
fwrite(submit,"./project/volume/data/processed/submit_glm.csv")
