# data wrangling
rm(list=ls())

# load the packages
library(data.table)
library(caret)
library(Rtsne)
library(Metrics)

# load in data 
train<-fread("./project/volume/data/interim/train.csv")
test <- fread("./project/volume/data/interim/test.csv")
train_emb <- fread("./project/volume/data/raw/train_emb.csv")
test_emb <- fread("./project/volume/data/raw/test_emb.csv")

#add a column that differentiates between train and test rows once they are together
test_emb$split<-0
train_emb$split<-1

#now bind them together

master<-rbind(train_emb,test_emb)
train_y<-master$split
drops <- c('split')
master <- master[, !drops, with = FALSE]

# do a pca
pca<-prcomp(master)
screeplot(pca)
summary(pca)

# use the unclass() function to get the data in PCA space
pca_dt<-data.table(unclass(pca)$x)


# run t-sne on the PCAs three times
tsne<-Rtsne(pca_dt,pca = F,perplexity=30,check_duplicates = F, max_iter = 5000, eta=150,  stop_lying_iter=1500)
tsne_2<-Rtsne(pca_dt,pca = F,perplexity=50,check_duplicates = F, max_iter = 5000, eta=150,  stop_lying_iter=1500)
tsne_3<-Rtsne(pca_dt,pca = F,perplexity=70,check_duplicates = F, max_iter = 5000, eta=150,  stop_lying_iter=1500)

# grab out the coordinates
tsne1<-data.table(tsne$Y)
tsne2<-data.table(tsne_2$Y)
tsne3<-data.table(tsne_3$Y)

# combind tsne results
tsne_dt <- cbind(tsne1, tsne2, tsne3)
tsne_dt$split<-train_y

# split back to train/test 
train_tsne<-tsne_dt[split==1]
test_tsne<-tsne_dt[split==0]

# clean up columns
train_tsne$split<-NULL
test_tsne$split<-NULL

# combind tsne data to train and test
train_bs <- cbind(train, train_tsne)
test_bs <- cbind(test, test_tsne)

# save the data
fwrite(train_bs,"./project/volume/data/interim/train_bs.csv")
fwrite(test_bs,"./project/volume/data/interim/test_bs.csv")
