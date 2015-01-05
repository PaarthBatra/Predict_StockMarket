## A module to do operations on SQllite db , Execute DDL , bhav csv to bhav table etc                                ##
## Licensed Freeware                                                                                                ##
## Author Paarth Batra                                                                                              ##
## Creation Date : 21st Oct 2013                                                                                    ##
## Last Update Date : 21st Oct 2013                                                                                 ##
##Example Call Bhavcsv_To_Sqlite3db('D:\Paarth\Codes\Python\Work\Database\csv_to_sqlite\sqllite_dbfiles','SMA_ANALYZER.DB','D:\Paarth\Codes\Python\Work\Database\csv_to_sqlite\Downloaded_Bhav_csv_files','2013OCT18.csv')  ##
## Version 1.0                                                                                                      ##

import csv, sqlite3
import Predict_StockMarket as p

def executeDDL(dbpath='',dbfilename='',ddl_sql=''):
    print "Parameters passed to me are : "
    print "Database file path : "+dbpath
    print "Database file name : "+dbfilename
    print "DDL SQL         : "+ddl_sql

    dbstr=dbpath+dbfilename
    try:
        con = sqlite3.connect(dbstr)
        cur = con.cursor()
        print "connection made to ",dbstr
    except Exception as E:
        print "Exception occurred while creating connection :",E
        return "Exception occurred while creating table ",E

    try:
        cur.execute(ddl_sql)
        print "SQL Processed Successfully"
    except Exception as E:
        print "Exception occurred while creating table ",E
        return "Exception occurred while creating table ",E
    con.close()
    return 0

def executeOneResultDML(dbpath='',dbfilename='',dml_sql=''):
    #This function will result only one row in 1 column i.e. only 1 value
    print "Parameters passed to me are : "
    print "Database file path : "+dbpath
    print "Database file name : "+dbfilename
    print "DDL SQL         : "+dml_sql

    dbstr=dbpath+dbfilename
    try:
        con = sqlite3.connect(dbstr)
        cur = con.cursor()
        print "connection made to ",dbstr
    except Exception as E:
        exceptionMsg="Exception occurred in connection Block :"+str(E)
        #print exceptionMsg
        return exceptionMsg
    data=[]
    try:
        print dml_sql
        cur.execute(dml_sql)
        resultValue= cur.fetchall()

    except Exception as E:
        exceptionMsg="Exception occurred in execution Block :"+str(E)
        #print exceptionMsg
        return exceptionMsg
    con.close()
    return  resultValue

def Bhavcsv_To_Sqlite3db(dbpath='',dbfilename='',csvpath='',csvfilename=''):
    dbStr=dbpath+dbfilename

    try:
        con = sqlite3.connect(dbStr)
        cur = con.cursor()
        print "connection made to ",dbStr
    except Exception as E:
        print "Exception occurred while creating connection :",E
        return "Exception occurred while creating table ",E

    csvFile=csvpath+csvfilename
    print "File name to read is ",csvFile


    with open(csvFile,'rb') as fileData: # `with` statement available in 2.5+
    # csv.DictReader uses first line in file for column headings by default
        dr = csv.DictReader(fileData) # comma is default delimiter
        to_db = [(i['SYMBOL'], i['SERIES'],i['OPEN'],i['HIGH'],i['LOW'],i['CLOSE'],i['LAST'],i['PREVCLOSE'],i['TOTTRDQTY'],i['TOTTRDVAL'],i['TIMESTAMP'],i['TOTALTRADES'],i['ISIN']) for i in dr]

    try:
        cur.executemany("INSERT INTO bhav (SYMBOL,SERIES,OPEN,HIGH,LOW,CLOSE,LAST,PREVCLOSE,TOTTRDQTY,TOTTRDVAL,TIMESTAMP,TOTALTRADES,ISIN) VALUES (?, ?,?,?,?,?,?,?,?,?,?,?,?);", to_db)
        con.commit()
    except Exception as E:
        exceptionMsg="Exception occurred while Inserting into DB :"+str(E)
        #print exceptionMsg
        return exceptionMsg
    return 0

def processBhavD_BV(dbpath='',dbfilename='',Date='20140116',tempTableName='',mainTableName=''):
    dbStr=dbpath+dbfilename

    print "processBhavD_VV : Values passed to me db_path = %s \n DB Name = %s \n Date = %s \n temptablename = %s \n maintablename = %s"%(dbpath,dbfilename,Date,tempTableName,mainTableName)
    try:
        con = sqlite3.connect(dbStr)
        cur = con.cursor()
        print "connection made to ",dbStr
    except Exception as E:
        print "Exception occurred while creating connection :",E

    Date = p.dateToBhavTimestamp(Date)
    print "Date now is ",Date
    SQL="insert into D_BV select SYMBOL,SERIES,OPEN,HIGH,LOW,CLOSE,LAST,PREVCLOSE,TOTTRDQTY,TOTTRDVAL," \
        "TIMESTAMP,TOTALTRADES,ISIN from BHAV " \
        "where series = 'EQ' and timestamp = '"+Date+"';"

    print SQL

    try:
        cur.execute(SQL)
        con.commit()
        return "Data Processed Successfully"
    except Exception as E:
        exceptionMsg="Exception occurred while Inserting into DB :"+str(E)
        #print exceptionMsg
        return exceptionMsg

def executeSelect(dbpath='',dbfilename='',select_sql=''):
    #This function will result only one row in 1 column i.e. only 1 value
    #print "Parameters passed to me are : "
    #print "Database file path : "+dbpath
    #print "Database file name : "+dbfilename
    #print "DDL SQL         : "+select_sql

    dbstr=dbpath+dbfilename
    try:
        con = sqlite3.connect(dbstr)
        cur = con.cursor()
        #print "connection made to ",dbstr
    except Exception as E:
        exceptionMsg="Exception occurred in connection Block :"+str(E)
        #print exceptionMsg
        return exceptionMsg

    data=[]
    try:

        cur.execute(select_sql)
        resultValue= cur.fetchall()
        col_name_list = [tuple[0] for tuple in cur.description]
        print col_name_list
    except Exception as E:
        exceptionMsg="Exception occurred in select_sql Block :"+str(E)
        #print exceptionMsg
        return exceptionMsg
    con.close()
    return  resultValue



#createBhavTableSQL="""create table BHAV (SYMBOL TEXT,SERIES TEXT,OPEN integer,HIGH integer,LOW
#                        integer,CLOSE	integer,LAST integer,PREVCLOSE integer,TOTTRDQTY integer,TOTTRDVAL integer,
#                        TIMESTAMP text,TOTALTRADES integer,ISIN text,Primary key(SYMBOL,SERIES,TIMESTAMP));"""

#create_MC_TableSQL="""create table MCdata (STOCK_NAME text,ANALYST_NAME text,DATE text,TIME text,SOURCE text,
#TIP text,ONELINER text,URL text,Primary key(STOCK_NAME,ANALYST_NAME ,DATE,TIME));"""


#dropBhavTableSQL="""drop table BHAV"""

#csvFilePath='D:\Paarth\Codes\Python\Work\QT\The SMA Analyzer\Downloaded_Bhav_csv_files'
#dbFilePath='D:\Paarth\Google_Drive\Google Drive\Codes\Python\Work\Web_URL\Analyst_Analyzer\Data\\bhav\\'
#dbFileName='SMA_ANALYZER.DB'
#csvFileName='2013OCT21.csv'

#Dropping existing Bhav table
#result=executeDDL(dbFilePath,dbFileName,dropBhavTableSQL)
#print"\n\nResult of running dropBhavtable is ",result

#creating new bhav table
#import Predict_StockMarket.config as cnf
#resultCreateTable=executeDDL(cnf.dbPath,cnf.dbName,createBhavTableSQL)
#print"\n\nResult of running createBhavTableSQL is ",resultCreateTable

#inserting data from csv

#resultCSVtosqlite=Bhavcsv_To_Sqlite3db(dbFilePath,dbFileName,csvFilePath,csvFileName)
#print"\n\nResult of running Bhavcsv_To_Sqlite3db is ",resultCSVtosqlite

#selectOneSQL="select distinct timestamp from BHAV"
#print executeOneResultDDL(dbFilePath,dbFileName,selectOneSQL)
