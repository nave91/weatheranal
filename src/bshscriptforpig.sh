#! /bin/bash                                                                  \
                                               
#Copy the script.pig and this script into the hadoop remote server before running it
                               
#Initialize year which is also the counter
YEAR=2000

#Initialize other variables which are arguments for the pig
I_INFO="s3://cs440-climate/gsod/"
I_STN="s3://cs440-climate/ish-history.csv"
OUTPUT="s3://cs440-nalekkalapudi/output/"

#Loop through 10 years
while [ $YEAR -le 2009 ]
do

    #Run the pig script with all the arguements
    pig -p YEAR=$YEAR -p I_INFO=$I_INFO -p I_STN=$I_STN -p OUTPUT=$OUTPUT file\
:///home/hadoop/script.pig
    
    #Increment the counter
    YEAR=$(( $YEAR + 1 ))

done
