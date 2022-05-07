rm(list = ls())

library(data.table)
library(caret)
library(Metrics)
library(glmnet)
library(plotmo)



#read in data, notice the path will always look like this because the assumed working directory is the repo level folder
train<-fread("./project/volume/data/interim/train_bs.csv")
test<-fread("./project/volume/data/interim/test_bs.csv")
submit<-fread("./project/volume/data/interim/test_bs.csv")

# subset out only the columns to model

drops<- c('Season','DayNum', 'team_1', 'team_2')

train<-train[, !drops, with = FALSE]
test<-test[, !drops, with = FALSE]

#save the response var because dummyVars will remove
train_y<-train$Pred

# work with dummies

dummies <- dummyVars(Pred ~ ., data = train)
train<-predict(dummies, newdata = train)
test<-predict(dummies, newdata = test)

train<-data.table(train)
test<-data.table(test)


# Use cross validation 
train<-as.matrix(train)
test<-as.matrix(test)

# ridge
gl_model<-cv.glmnet(train, train_y, alpha = 0,family="binomial")
bestlam<-gl_model$lambda.1se


#fit the full model
gl_model<-glmnet(train, train_y, alpha = 0,family="binomial")

plot_glmnet(gl_model)

#save model
saveRDS(gl_model,"./project/volume/models/gl_model.model")

test<-as.matrix(test)


#use the full model
pred<-predict(gl_model,s=bestlam, newx = test,type = "response")

# make a submision file 
submit$ID <- paste(submit$Season, submit$team_1, submit$team_2, sep = "_")
submit$Pred<-pred
submit <- submit[, .(ID, Pred)]


fwrite(submit,"./project/volume/data/processed/submit.csv")
