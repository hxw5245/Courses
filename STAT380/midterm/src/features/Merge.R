rm(list = ls())
library(data.table)


#read in data
stats_by_day <- fread('./project/volume/data/interim/stats_by_day.csv')
test <- fread('./project/volume/data/interim/test.csv')
train <- fread('./project/volume/data/interim/train.csv')

master <- rbind(train,test)

# merge
tmp <- merge(master, stats_by_day, by.x = c("team_1", "Season", "DayNum"), by.y=c("TeamID", "Season", "DayNum"), all.x = T, sort=FALSE)
master <- merge(tmp, stats_by_day, by.x = c("team_2", "Season", "DayNum"), by.y=c("TeamID", "Season", "DayNum"), all.x = T, sort=FALSE)

master$Score_dif <- master$Score.x - master$Score.y
master$FGM_dif <- master$FGM.x - master$FGM.y
master$FGA_dif <- master$FGA.x - master$FGA.y
master$FGM3_dif <- master$FGM3.x - master$FGM3.y
master$FGA3_dif <- master$FGA3.x - master$FGA3.y
master$FTM_dif <- master$FTM.x - master$FTM.y
master$FTA_dif <- master$FTA.x - master$FTA.y
master$OR_dif <- master$OR.x - master$OR.y
master$DR_dif <- master$DR.x - master$DR.y
master$Ast_dif <- master$Ast.x - master$Ast.y
master$TO_dif <- master$TO.x - master$TO.y
master$Stl_dif <- master$Stl.x - master$Stl.y
master$Blk_dif <- master$Blk.x - master$Blk.y
master$PF_dif <- master$PF.x - master$PF.y


#- get rid of id variables and nas 
master <- master[,.(Season, DayNum, team_1, team_2, Pred, POM_dif,SAG_dif, MOR_dif, DOK_dif, 
                    Score_dif, FGM_dif, FGA_dif, FGM3_dif, FGA3_dif, FTM_dif, FTA_dif, OR_dif, 
                    DR_dif, Ast_dif, TO_dif, Stl_dif, Blk_dif, PF_dif)]
master <- na.omit(master)

# split the data
test_bs <- master[Pred == 0.5]
train_bs <- master[Pred != 0.5]



fwrite(test_bs,'./project/volume/data/interim/test_bs.csv')
fwrite(train_bs,'./project/volume/data/interim/train_bs.csv')
