rm(list = ls())
library(data.table)

test <- fread('./project/volume/data/raw/MSampleSubmissionStage2.csv')
season <- fread('./project/volume/data/raw/MRegularSeasonDetailedResults.csv')
tourney <- fread('./project/volume/data/raw/MNCAATourneyDetailedResults.csv')
ranks <- fread('./project/volume/data/raw/MMasseyOrdinals.csv')

#- Clean test

test <- data.table(matrix(unlist(strsplit(test$ID,"_")),ncol=3,byrow=T))
setnames(test,c("V1","V2", "V3"),c("Season","team_1","team_2"))

test$DayNum <- max(season[Season == 2021,DayNum]) + 1
test$Pred <- 0.5

#- initializing train

train <- rbind(season,tourney)
train <- train[,.(WTeamID,LTeamID,Season,DayNum)]
setnames(train,c("WTeamID","LTeamID"),c("team_1","team_2"))

train$Pred <- 1

#- make master data file

master <- rbind(train,test)

#- ensure my team ids are characters
master$team_1 <- as.character(master$team_1)
master$team_2 <- as.character(master$team_2)

#- teams' rank often change the day of a game so don't want to use the 'future'
# values. we ofset them by one.
ranks$DayNum <- ranks$RankingDayNum+1

#- creating a loop to add them into the table

which_system <- c("POM","SAG","MOR","DOK")
master$Season <- as.integer(master$Season)

#- subset the ranks table
for(i in 1:length(which_system)){
one_rank <- ranks[SystemName == which_system[i]][,.(Season,DayNum,TeamID,OrdinalRank)]

#- prep and join into the first team
setnames(one_rank,"TeamID","team_1")
one_rank$team_1 <- as.character(one_rank$team_1)
setkey(master,Season,team_1,DayNum)
setkey(one_rank,Season,team_1,DayNum)

#- join here
master <- one_rank[master,roll=T]
setnames(master,"OrdinalRank","team_1_rank")


#- prep and merge into the second team
setnames(one_rank,"team_1","team_2")
setkey(master,Season,team_2,DayNum)
setkey(one_rank,Season,team_2,DayNum)

master <- one_rank[master,roll=T]
setnames(master,"OrdinalRank","team_2_rank")

#subtract the rankings for a new variable
master$rank_dif <- master$team_2_rank-master$team_1_rank

master$team_1_rank <- NULL
master$team_2_rank <- NULL
setnames(master,"rank_dif",paste0(which_system[i],"_dif"))
}



#- clean up the data
master <- master[order(Season,DayNum)]


#- get rid of id variables and nas ( you should keep the ids, Season and Day)
master <- master[,.(Season,DayNum, team_1,team_2,POM_dif, SAG_dif, MOR_dif, DOK_dif, Pred)]
master <- na.omit(master)

#split back into train and test
test <- master[Pred == 0.5]
train <- master[Pred == 1]

#- divide so I have losses 
rand_inx <- sample(1:nrow(train),nrow(train)*0.5)
train_a <- train[rand_inx,]
train_b <- train[!rand_inx,]

#- train_b will encode the loses
train_b$Pred <- 0
train_b$POM_dif <- train_b$POM_dif*-1
train_b$SAG_dif <- train_b$SAG_dif*-1
train_b$MOR_dif <- train_b$MOR_dif*-1
train_b$DOK_dif <- train_b$DOK_dif*-1

setnames(train_b,c("team_1","team_2"),c("team_2","team_1"))

train <- rbind(train_a,train_b)

fwrite(test,'./project/volume/data/interim/test.csv')
fwrite(train,'./project/volume/data/interim/train.csv')

