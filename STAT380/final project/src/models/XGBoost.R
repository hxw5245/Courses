# data wrangling
rm(list=ls())

# load the packages
library(data.table)
library(caret)
library(Rtsne)
library(xgboost)
library(Metrics)


# load in data 
train<-fread("./project/volume/data/interim/train_bs.csv")
test <- fread("./project/volume/data/interim/test_bs.csv")
example_sub <- fread("./project/volume/data/raw/example_sub.csv")

y.train <- train$category
y.test <- test$category

# change column names and drop id column
setnames(train, c('id', 'category', 'V1', 'V2', 'V3', 'V4', 'V5', 'V6'))
setnames(test, c('id','category','V1', 'V2', 'V3', 'V4', 'V5', 'V6'))

drops <- c('id')
train <- train[, !drops, with = FALSE]
test <- test[, !drops, with = FALSE]

# do dummy variables and convert to matrix
dummies <- dummyVars(category ~ ., data = train)
x.train<-predict(dummies, newdata = train)
x.test<-predict(dummies, newdata = test)

dtrain<-xgb.DMatrix(x.train, label=y.train,missing=NA)
dtest<-xgb.DMatrix(x.test,missing=NA)

#- intialize my hyper parameters
hyper_parm_tune <- NULL

# Set necessary parameter
eta <- c(0.01, 0.05, 0.1, 0.15, 0.2, 0.25, 0.3, 0.5)
max_depth <- c(5, 8, 10, 15, 20)
subsample <- c(0.7, 0.8, 0.9)
colsample_bytree <- c(0.7, 0.8, 0.9)
min_child_weight <- c(0.5, 1, 5)
nfold <- c(4, 5, 6)

for(i in eta){
  for(j in max_depth){
    for (k in subsample){
      for(l in colsample_bytree){
        for (m in min_child_weight){
          for (n in nfold){
            param <- list(   objective           = "multi:softprob", 
                             gamma               = 0.02,
                             booster             = "gbtree",
                             eval_metric         = "mlogloss",
                             eta                 = i, 
                             num_class           = 10,
                             min_child_weight    = m,
                             max_depth           = j,
                             subsample           = k,
                             colsample_bytree    = l)
            
            XGBfit <- xgb.cv(params = param, 
                             nfold =n, 
                             nrounds = 100000, 
                             data = dtrain,
                             missing = NA,
                             print_every_n = 1, 
                             early_stopping_rounds = 25)
            
            best_tree_n <- unclass(XGBfit)$best_iteration
            new_row <- data.table(t(param))
            new_row$best_tree_n <- best_tree_n
            test_error <- unclass(XGBfit)$evaluation_log[best_tree_n,]$test_mlogloss_mean
            new_row$test_error <- test_error
            hyper_parm_tune <- rbind(new_row, hyper_parm_tune)
          }
        }
      }
    }
  }
}
#----------------------------------#
# fit the model to all of the data #
#----------------------------------#
hyper_parm_tune[which.min(hyper_parm_tune$test_error),]
bst_tree <- 102
watchlist <- list(train = dtrain)

XGBfit <- xgb.train(params = param, 
                    nrounds = bst_tree, 
                    data = dtrain,
                    missing = NA,
                    watchlist = watchlist,
                    print_every_n = 1)
pred <- predict(XGBfit, newdata=dtest,missing=NA)

# reformat
results <- matrix(pred,ncol=10,byrow=T)
results <- as.data.frame(t(results))
results <- transpose(results)

setnames(results, old = c('V1', 'V2', 'V3', 'V4', 'V5', 'V6', 'V7', 'V8', 'V9', 'V10'), 
         new = c('subredditcars', 'subredditCooking', 'subredditMachineLearning', 'subredditmagicTCG', 
                 'subredditpolitics', 'subredditReal_Estate', 'subredditscience', 'subredditStockMarket', 
                 'subreddittravel', 'subredditvideogames'))

# submit
results$id <- example_sub$id
results <- results[, c(11, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)]

fwrite(results,"./project/volume/data/processed/submit.csv")
