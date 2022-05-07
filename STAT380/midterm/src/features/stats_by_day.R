rm(list = ls())
library(data.table)

#read in data
regular <- fread('./project/volume/data/raw/MRegularSeasonDetailedResults.csv')
tourney <- fread('./project/volume/data/raw/MNCAATourneyDetailedResults.csv')

all_games_table <- rbind(regular, tourney)
  
W_stats<-all_games_table[,.(Season, DayNum, WTeamID, WScore, WFGM, WFGA, WFGM3, WFGA3, 
                   WFTM, WFTA, WOR, WDR, WAst, WTO, WStl, WBlk, WPF)]
L_stats<-all_games_table[,.(Season, DayNum, LTeamID, LScore, LFGM, LFGA, LFGM3, LFGA3, 
                   LFTM, LFTA, LOR, LDR, LAst, LTO, LStl, LBlk, LPF)]

setnames(W_stats, old = c('WTeamID', 'WScore', 'WFGM', 'WFGA', 'WFGM3', 'WFGA3', 
                          'WFTM', 'WFTA', 'WOR', 'WDR', 'WAst', 'WTO', 'WStl', 'WBlk', 'WPF'), 
         new = c('TeamID', 'Score', 'FGM', 'FGA', 'FGM3', 'FGA3', 
                 'FTM', 'FTA', 'OR', 'DR', 'Ast', 'TO', 'Stl', 'Blk', 'PF'))

setnames(L_stats, old = c('LTeamID', 'LScore', 'LFGM', 'LFGA', 'LFGM3', 'LFGA3', 
                          'LFTM', 'LFTA', 'LOR', 'LDR', 'LAst', 'LTO', 'LStl', 'LBlk', 'LPF'), 
         new = c('TeamID', 'Score', 'FGM', 'FGA', 'FGM3', 'FGA3', 
                 'FTM', 'FTA', 'OR', 'DR', 'Ast', 'TO', 'Stl', 'Blk', 'PF'))

master_stats <- rbind(W_stats, L_stats)

stats_by_day<-NULL

for (i in 1:max(master_stats$DayNum)){
  sub_master <- master_stats[DayNum < i]
  team_stats_by_day <- dcast(sub_master, TeamID + Season~., mean, 
                             value.var=c('Score', 'FGM', 'FGA', 'FGM3', 'FGA3', 'FTM', 'FTA', 
                                         'OR', 'DR', 'Ast', 'TO', 'Stl', 'Blk', 'PF'))
  team_stats_by_day$DayNum <- i
  stats_by_day<-rbind(stats_by_day,team_stats_by_day)
}

fwrite(stats_by_day,'./project/volume/data/interim/stats_by_day.csv')
