#!/bin/bash

#Supply R script name as first arguement and directory containing individual years of output as second arguement

#Initialize the script R
scriptforR=$1

#create a directory for final output
mkdir fout

#enter into the directory which has the output
cd $2/

#intialize year
yr=2000

#Loop through all the years
while [ $yr -le 2009 ]; do

#enter into "year" directory
cd $yr

#Run the R script for individual part files
Rscript ../../$scriptforR part*

#Run the rename command for changing names of pdf to year
mv -v *.pdf $yr.pdf

#copy pdfs to fout directory
mv -v $yr.pdf ../../fout/

#Traverse back to parent and continue in loop
cd ../

#increment counter
yr=$(($yr+1))

done