# load in libraries
rm(list = ls())

library(xgboost)
library(caret)
library(data.table)
library(Metrics)

train<-fread('./project/volume/data/raw/Stat_380_train.csv')
test<-fread('./project/volume/data/raw/Stat_380_test.csv')

train$Id<-NULL
test$Id<-NULL

y.train<-train$SalePrice

test$SalePrice<-0
y.test<-test$SalePrice

dummies <- dummyVars(SalePrice ~ ., data = train)
x.train<-predict(dummies, newdata = train)
x.test<-predict(dummies, newdata = test)


dtrain<-xgb.DMatrix(x.train, label=y.train,missing=NA)
dtest<-xgb.DMatrix(x.test,missing=NA)

#- intialize my hyper parameters
hyper_parm_tune <- NULL

#---------------------------------#
#     Use cross validation        #
#---------------------------------#

#- create my parameters
myparam <- list( objective           = "reg:squarederror", 
                 gamma               =0.02,
                 booster             = "gbtree",
                 eval_metric         = "rmse",
                 eta                 = 0.01,
                 max_depth           = 5,
                 min_child_weight = 1,
                 subsample           = 0.6,
                 colsample_bytree    = 0.7,
                 tree_method = 'hist')
XGBfit <- xgb.cv(params = myparam, 
                 nfold =5, 
                 nrounds = 100000, 
                 missing = NA, 
                 data = dtrain, 
                 print_every_n = 1, 
                 early_stopping_rounds = 25)

best_tree_n <- unclass(XGBfit)$best_iteration
new_row <- data.table(t(myparam))
new_row$best_tree_n <- best_tree_n
test_error <- unclass(XGBfit)$evaluation_log[best_tree_n,]$test_rmse_mean
new_row$test_error <- test_error
hyper_parm_tune <- rbind(new_row, hyper_parm_tune)

fwrite(hyper_parm_tune,"./project/volume/data/processed/hyper_parm_tune.csv")

#----------------------------------#
# fit the model to all of the data #
#----------------------------------#
watchlist <- list(train = dtrain)

XGBfit <- xgb.train(params = myparam, 
                    nrounds = best_tree_n, 
                    missing = NA, 
                    data = dtrain,
                    watchlist = watchlist,
                    print_every_n = 1)
pred <- predict(XGBfit, newdata=dtest,missing=NA)
rmse(y.test, pred)

# submit
test<-fread('./project/volume/data/raw/Stat_380_test.csv')

test<-test[,.(Id)]
test$SalePrice<-pred


fwrite(test,"./project/volume/data/processed/Submit.csv")
