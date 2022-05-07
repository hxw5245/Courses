# data wrangling
rm(list=ls())

# load the packages
library(data.table)
library(Rtsne)
library(caret)
library(ClusterR)


# load in data 
data<-fread("./project/volume/data/raw/data.csv")

drops<- c('id')

data<-data[, !drops, with = FALSE]

# do a pca
pca<-prcomp(data)
screeplot(pca)
summary(pca)

# use the unclass() function to get the data in PCA space
pca_dt<-data.table(unclass(pca)$x)


# run t-sne on the PCAs
tsne<-Rtsne(pca_dt,pca = F,perplexity=70,check_duplicates = F, max_iter = 5000, eta=150,  stop_lying_iter=1500)

# grab out the coordinates
tsne_dt<-data.table(tsne$Y)

# use a gaussian mixture model

k_aic<-Optimal_Clusters_GMM(pca_dt[,1:3], max_clusters = 10,criterion = "AIC")
delta_k<-c(NA,k_aic[-1] - k_aic[-length(k_aic)])

best_clus_num <- 3
gmm_data<-GMM(pca_dt[,1:3],best_clus_num)

 
# convert log-likelihood into a probability

l_clust<-gmm_data$Log_likelihood^10

l_clust<-data.table(l_clust)

net_lh<-apply(l_clust,1,FUN=function(x){sum(1/x)})

cluster_prob<-1/l_clust/net_lh

# submit
submit<-fread("./project/volume/data/raw/data.csv")
submit$breed.3<-cluster_prob$V1
submit$breed.2<-cluster_prob$V2
submit$breed.1<-cluster_prob$V3
submit<-submit[,.(id, breed.1, breed.2, breed.3)]

fwrite(submit,"./project/volume/data/processed/submit.csv")
