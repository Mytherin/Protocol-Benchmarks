library(dplyr)
library(ggplot2)
library(ggthemes)
library(ggrepel)
library(stringr)
library(grid)
library(reshape2)


theme <- theme_few(base_size = 24) + 
theme(axis.title.y=element_text(vjust=0.9), 
  axis.title.x=element_text(vjust=-0.1),
  axis.ticks.x=element_blank(),
  text=element_text(family="serif"))


sysnames <- c("mariadb-default"="MySQL", "mariadb-compress"="MySQL+C", "oracle-default"="DBMS X", "postgres-default" = "PostgreSQL", "db2-default" = "DB2", "monetdb-prot9"="MonetDB", "hive-default" = "Hive", "mongodb-default" = "MongoDB")


# FIRST GRAPH: lineitem 1 million rows for all systems
read.table("lineitem1m.csv", header=T,  sep=",", stringsAsFactors=F, na.strings="-1") -> lineitem1mdata


lineitem1mdata %>% filter(grepl("-connectonly", system)) %>% mutate(system=sub("-lineitem", "", sub("-connectonly","",system))) %>% group_by(system, network) %>% summarise(time_mean = mean(time)) %>% select(system,connecttime=time_mean) -> connect
lineitem1mdata %>% filter(grepl("-nofetch", system)) %>% mutate(system=sub("-lineitem", "", sub("-nofetch","",system))) %>% group_by(system, network) %>% summarise(time_mean = mean(time)) %>% select(system,exectime=time_mean) -> execute
lineitem1mdata %>% filter(grepl("-noprint", system)) %>% mutate(system=sub("-lineitem", "", sub("-noprint","",system))) %>% group_by(system, network) %>% summarise(time_mean = mean(time)) %>% select(system,transfertime=time_mean) -> transfer
lineitem1mdata %>% filter(grepl("-print", system)) %>% mutate(system=sub("-lineitem", "", sub("-print","",system))) %>% group_by(system, network) %>% summarise(time_mean = mean(time)) %>% select(system,printtime=time_mean) -> print

varmap = c("connecttime"="Connection", "exectime"="Query Execution", "printtime"="Printing", "transfertime"="RSS + Transfer")
print %>% left_join(transfer) %>% mutate(printtime=printtime-transfertime) %>% left_join(execute) %>% mutate(transfertime=transfertime-exectime) %>% left_join(connect) %>% mutate(exectime=exectime-connecttime) -> joined


joined %>% melt(id.var="system") %>% mutate(system=sysnames[system]) -> almostfinal

joined %>% mutate(totaltime=printtime+connecttime+exectime+transfertime) %>% select(system,totaltime) -> systimes

time_mapping <- function(df) {
	df %>% group_by(system) %>% summarise(totaltime = round(sum(value),1)) -> systimes
	systimes %>% left_join(df) %>% mutate(value=totaltime) %>% select(system,variable,value)
}

# adjust theme for additional plot margin to make room for text
theme <- theme_few(base_size = 24) + 
theme(axis.title.y=element_text(vjust=0.9), 
  axis.title.x=element_text(vjust=-0.1),
  axis.ticks.x=element_blank(),
  text=element_text(family="serif"),
  plot.margin=unit(c(1,0.5,0,0), "cm"),
  legend.position = c(0.85, 0.8))

ordering <- c("connecttime"=1,"exectime"=2,"transfertime"=3,"printtime"=4)
almostfinal[with(almostfinal, order(-ordering[variable])),] %>% mutate(value=ifelse(value < 0, 0, value), variable=varmap[as.character(variable)]) -> final

levels(final$variable) <- c(as.character(varmap["connecttime"]), as.character(varmap["exectime"]), as.character(varmap["transfertime"]),as.character(varmap["printtime"]))

p <- ggplot(final, aes(x = reorder(system, -value), y = value, fill = variable, label=value)) + geom_bar(stat = "identity", width=.7) + theme + xlab("") + geom_hline(yintercept=0.23, linetype="dashed", size=1) + ylab("Wall clock time (s)") + coord_flip() + scale_y_continuous(limits=c(0, 26)) + annotation_custom(grob = textGrob("Netcat (0.23s)",gp = gpar(fontsize = 19)),  xmin = -1, xmax = 19.2, ymin = 0.23, ymax = 0.23) + scale_fill_manual(name="Operation",values=c("#228B22","#EEAD0E","#B22222","#6495ED")) + geom_text(data=time_mapping,size=7, hjust=-.3, family="serif")

pdf("total-time-bars.pdf", width=10, height=5)

gt <- ggplot_gtable(ggplot_build(p))
gt$layout$clip[gt$layout$name == "panel"] <- "off"
grid.draw(gt)

dev.off()

# final table: all three datasets for all systems + new protocol
read.table("finalalldatasets-old.csv", header=T,  sep=",", stringsAsFactors=F, na.strings="-1") -> finaltest
sysnames <- c("mariadb-default"="MySQL", "mariadb-compress"="MySQL+C", "oracle-default"="DBMS X", "postgres-default" = "PostgreSQL", "db2-default" = "DB2", "monetdb-prot9"="MonetDB", "hive-default" = "Hive", "mongodb-default" = "MongoDB", "monetdb-prot10-snappy"="NewProto+C", "monetdb-prot10"="NewProto", "netcat-csv"="Netcat")
datasetnames <- c("lineitem"="Lineitem", "acs3yr"="ACS","ontime"="Ontime")
dataset_ordering = c("Lineitem"=1,"ACS"=2,"Ontime"=3)
system_ordering = c("Netcat"=1, "NewProto"=2, "NewProto+C"=3,"MySQL"=4,"MySQL+C"=5,"PostgreSQL"=6,"DB2"=7,"DBMS X"=8, "Hive"=9, "MongoDB")

finaltest %>% filter(timeout==0) %>% group_by(system,network,dataset,tuple) %>% summarise_each(funs(median)) -> summarized_final

summarized_final %>% filter(tuple!=1) %>% select(time, bytes) %>% left_join(summarized_final %>% filter(tuple==1) %>% select(time, bytes), by=c("system","network","dataset")) %>% mutate(time=time.x-time.y,bytes=bytes.x-bytes.y) %>% select(time,bytes) -> adjusted_results

adjusted_results %>% filter(network=="unlimited") %>% left_join(adjusted_results %>% filter(network=="gigabitlhd"), by=c("system","dataset")) %>% left_join(adjusted_results %>% filter(network=="100mbitlhd"), by=c("system","dataset")) %>% as.data.frame() %>% mutate(system=sysnames[sub("-lineitem","",sub("-ontime","",sub("-acs3yr","",system)))],dataset=datasetnames[dataset],bytes=round(bytes.x/1024/1024,1)) %>% select(dataset,system,local=time.x,lan=time.y,wan=time,bytes) %>% mutate(local=ifelse(local < 10, round(local,2), round(local,1)), lan=ifelse(lan < 10, round(lan,2), round(lan,1)), wan=ifelse(wan < 10, round(wan,2), round(wan,1)))-> final
final[with(final, order(dataset_ordering[dataset], system_ordering[system])),] -> ordered_final

  nrows = 10
  ordered_final$dataset <- ""
  ordered_final$dataset[1] <- paste0("\\multirow{",nrows,"}{*}{\\rotatebox[origin=c]{90}{Lineitem}}")
  ordered_final$dataset[1 + nrows] <- paste0("\\multirow{",nrows,"}{*}{\\rotatebox[origin=c]{90}{ACS}}")
  ordered_final$dataset[(1 + nrows + nrows)] <- paste0("\\multirow{",nrows,"}{*}{\\rotatebox[origin=c]{90}{Ontime}}")


  colnames(ordered_final) <- c("", "System", "$T_{Local}$", "$T_{LAN}$", "$T_{WAN}$", "Size (MB)")
  tbl <- print(xtable::xtable(ordered_final), include.rownames=FALSE, sanitize.colnames.function = identity, sanitize.text.function = identity)
