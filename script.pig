
--Pig Script for joining two tables FILTERED_BASE_STN and FILTERED_BASE_INFO 
----------------------------------------------------------------------------


--Loads .csv file which has stn, state and country
RAW_STN = LOAD '$I_STN' USING TextLoader as (line:chararray);

--Register the file to use functions in piggybank
register file:///home/hadoop/lib/pig/piggybank.jar

--Give an alias to extract function
DEFINE EXTRACT org.apache.pig.piggybank.evaluation.string.EXTRACT();

--Split Raw_STN into tuples by giving external type cast as tuple() in order to help hadoop recognize the type of tuple we are dealing with
--This helps with the AVG OR SUM functions
BASE_STN = foreach RAW_STN generate FLATTEN((tuple(chararray,chararray,chararray,chararray,chararray,chararray,chararray,chararray,chararray,chararray))EXTRACT(line, '^"(\\d+)","(\\d+)","(|\\S+[ \\S+]*)","([A-Z][A-Z]|)","([A-Z][A-Z]|)","([A-Z][A-Z]|)","(\\S+|)","(\\S+|)","(\\S+|)","(\\S+|)"$')) as (STN:chararray, WBAN:chararray, STNNAME:chararray, CNTRY:chararray,FIPS:chararray,ST:chararray,CALL:chararray,LON:chararray,LAT:chararray,ELEV:chararray);


--Filter BASE_STN such that it doesnt have any empty tuples or nulls and also give only US entries
TABLE_STN = FILTER BASE_STN BY CNTRY matches '[U][S]' AND STN matches '\\S+' AND WBAN matches '\\S+' AND ST matches '[A-Z][A-Z]';

--------------------------------------------------------------------------------------------------


--Loads all data (including directories into RAW_INFO) into RAW_INFO My guess is in form of bytes
RAW_INFO = LOAD '$I_INFO$YEAR' USING TextLoader as (line:chararray);

--Split Raw_INFO into tuples by giving external type cast as tuple() in order to help hadoop recognize the type of tuple we are dealing with
--This helps with the AVG OR SUM functions
BASE_INFO = foreach RAW_INFO generate
FLATTEN
(
	(
		tuple 
			(chararray,chararray,chararray,chararray,chararray,double,int,double,int,double,int,double,int,double,int,double,int,double,double,double,double,double,double,int,int,int,int,int,int)
	) 
	EXTRACT
		(line, '^(\\d+)[ ]*(\\d+)[ ]*([0-9]{4})([0-9]{2})([0-9]{2})[ ]*(\\S+)[ ]*(\\S+)[ ]*(\\S+)[ ]*(\\S+)[ ]*(\\S+)[ ]*(\\S+)[ ]*(\\S+)[ ]*(\\S+)[ ]*(\\S+)[ ]*(\\S+)[ ]*(\\S+)[ ]*(\\S+)[ ]*(\\S+)[ ]*(\\S+)[ ]*(\\S+)\\S[ ]*(\\S+)\\S[ ]*(\\S+)\\S[ ]*(\\S+)[ ]*([0-9])([0-9])([0-9])([0-9])([0-9])([0-9])')
) 
as
(STN:chararray, WBAN:chararray, YEAR:chararray, MO:chararray, DA:chararray, TEMP:double, TEMPCOUNT:int, DEWP:double, DEWPCOUNT:int, SLP:double, SLPCOUNT:int, STP:double, STPCOUNT:int, VISIB:double, VISIBCOUNT:int, WDSP:double, WDSPCOUNT:int, MXSPD:double, GUST:double, MAX:double, MIN:double, PRCP:double, SNDP:double, FOG:int, RAIN:int, SNOW:int, HAIL:int, THUNDER:int, TORNADO:int);

--Filter BASE_INFO such that it doesnt have any empty tuples and also remove any null values
TABLE_INFO = FILTER BASE_INFO BY YEAR matches '.*19.*' OR YEAR matches '.*20.*' AND STN matches '\\S+' AND WBAN matches '\\S+';


---------------------------------------------------------------------------------------------------

--Defining MACRO to filter out values and join with TABLE_STN to get AVG info about stations

DEFINE my_filter_avg(A,TABLE_STN,STN,WBAN,ST,YEAR,MO,DA,filter_var,filter_key) RETURNS D {

    --Filter the values 9999.9 or missing as specified in the readme.txt
    B = FILTER $A BY $filter_var != $filter_key;

    --Join above filtered table with the stn table which has the information regarding each station
    C = join B by ($WBAN,$STN), $TABLE_STN by ($WBAN,$STN);

    --generate average temp for a day by grouping by State, Year, Month, and Date
    $D = FOREACH (GROUP C BY ($ST,$YEAR,$MO,$DA)) GENERATE FLATTEN(group), AVG(C.$filter_var);
}

---------------------------------------------------------------------------------------------------

--Generate a table for average temperature for a state in a year in a month for each day
TFORTEMP = my_filter_avg(TABLE_INFO,TABLE_STN,WBAN,STN,ST,YEAR,MO,DA,TEMP,9999.9);

--Store output in a folder specified by its corresponding year
STORE TFORTEMP into '$OUTPUT$YEAR';
