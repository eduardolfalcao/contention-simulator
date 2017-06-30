library(ggplot2)
load_data <- function(path, pattern) { 
  fullname = paste(path,pattern,sep='',collapse='')
  files = Sys.glob(fullname)
  tables <- lapply(files, read.csv)
  do.call(rbind, tables)
}

library(plyr)
library(reshape2)

nPeers=10
nUsers=2
nClusters=3
path = "/home/eduardolfalcao/git/contention-simulator/ContentionSimulator/data/output/"
search = paste0("peerCapacity*-users",nUsers,"-peers",nPeers,"-clusters",nClusters,".csv")

contention = load_data(path,search)
ggplot()+
  geom_line(data = contention, aes(x=t, y=kappa)) + 
  facet_grid(capacity~.) + theme_bw() 


png("/home/eduardolfalcao/Área de Trabalho/Dropbox/Doutorado/Disciplinas/Projeto de Tese 5/workload-generator/tool/contention/contention.png", width=1200, height=1000)

dev.off()
