#Load arguments
args<-commandArgs(TRUE)
batch_args<-read.table(args[1], sep=",")

#Load the year wise data into the dataframe
data<-read.table(args[1])

#initialize variables for title
state<-"STATE:"
year<-"YEAR:"
month<-"MONTH:"

#load single instance of each state into a seperate vector
list_v1<-levels(factor(data$V1))

#loop through all the states
for(st in list_v1){

	#Load the data of each state into a temporary data frame
        temp_st<-data[data$V1==st,]

	#load single instance of each month into a seperate vector
	list_v3<-levels(factor(temp_st$V3))

	#loop through all the states
       	for(mon in list_v3){

       	       #Load the data of each month of a state into a temporary data frame
	       temp_mon<-temp_st[temp_st$V3==mon,]

               #Prepare title for the graph
	       title<-paste(state,levels(factor(temp_mon$V1)),year,levels(factor(temp_mon$V2)),month,levels(factor(temp_mon$V3)))

	       #Plot a bar graph depicting temperatures over a month for a state
	       barplot(temp_mon$V5,temp_mon$V4,names.arg=c(temp_mon$V4),xlab="Days",ylab="Temp",main=title,axis.lty=1,col="#6E8B3D",width=1,legend=NULL,space=NULL)

	}
}
