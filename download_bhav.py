__author__ = 'pbatra'
import httplib
import zipfile
import StringIO
import calendar
import Predict_StockMarket.config as cnf
import datetime
import Database.SQLiteDBOperations as d




#Function to find next date in int from int according to the parameter given
def dateAfterDays(startDate='',numOfDays=1):
    #print "Hi I am dateAfterDays Function ."
    #print "Paramaters passed are "
    #print "startDate : ",startDate
    #print "numOfDays :",numOfDays
    #givenDateInt=datetime.datetime.strptime(str(startDate),'%Y%m%d').date().strftime('%Y%m%d')
    j=datetime.datetime.strptime(str(startDate),'%Y%m%d').date()+datetime.timedelta(days=numOfDays)

    returnValue= j.strftime('%Y%m%d')
    #print "value ",returnValue
    return returnValue


def intToDateYMD(intDate=''):
    #print "int date provided is ",intDate
    returnValue=startDateDate=datetime.datetime.strptime(str(intDate),'%Y%m%d').date().strftime('%Y%m%d')
    #print returnValue
    return returnValue


def getDisplayStr(P_Date):
    #print "Hi i am get Disply Str i will take date as input and give result as for 12th oct 2013 , Saturday 12 October 2013"
    Date=P_Date

    dayinStr=str(datetime.datetime.strptime(Date,'%Y%m%d').strftime('%A'))
    dayinNum=str(datetime.datetime.strptime(Date,'%Y%m%d').day)
    monthname=calendar.month_name[datetime.datetime.strptime(Date,'%Y%m%d').month]
    year=str(datetime.datetime.strptime(Date,'%Y%m%d').year)
    #print "day is ",dayinNum
    #print "monthname is ",monthname
    returnstr=dayinStr +", "+dayinNum+" "+ monthname+" "+year
    return  returnstr


def getCSVFile(m=10, year=2010, dd=07, path_file = ""):
    #returns 0 on success and 1 on error
    """ Downloads the CSV Bhavcopy for the date and path given """

    #Convert Number to Month in abbrevated form i.e. 10 to OCT and 1 to JAN
    month=calendar.month_abbr[m].upper()
    bhavDownloadURL=cnf.bhavDownloadURL

    #Create Connection
    conn = httplib.HTTPConnection(bhavDownloadURL)
    #Create request URL
    reqstr = "/content/historical/EQUITIES/%d/%s/cm%02d%s%dbhav.csv.zip" % (year, month, dd, month, year)
    #CReate Header
    headers =   {   'User-Agent':'Mozilla/5.0 (Windows; U; Windows NT 6.0; en-US) AppleWebKit/534.7 (KHTML, like Gecko) Chrome/7.0.517.44 Safari/534.7',
                    'Accept':'application/xml,application/xhtml+xml,text/html;q=0.9,text/plain;q=0.8,image/png,*/*;q=0.5',
                    'Accept-Encoding':'gzip,deflate,sdch',
                    'Referer':'http://www.nseindia.com/archives/archives.htm'}

    # HTTPConnection.request(method, url[, body[, headers]])
    # Here method is GET , url is reqstr, body is none and headers is headers
    conn.request("GET", reqstr, None, headers)
    #get response in response var
    response = conn.getresponse()

    if response.status != 200:
        #print "Response status from nse.com",response.status
        return response.status


    #Reading response ind ata
    data = response.read()

    #get data in string
    sData = StringIO.StringIO(data)

    #Read data
    z = zipfile.ZipFile(sData)

    #get data as csv
    csv = z.read(z.namelist()[0])

    #create filename
    filename=path_file+str(year)+month+str(dd)+".csv"
    #str(year)+month+str(dd)
    #create file and write a name
    file=open(filename,'w')
    file.write(csv)
    return 0

def dateBVformat(P_Date):
    #print "Hi i will get 20140115 as input and will output 15 Jan 2015
    Date=P_Date
    return Date
#Pass Month,Year , Date and path to save file to getCSVFile(10,2013,18,'D:\Paarth\Codes\Python\Work\Database\csv_to_sqlite\downloaded_Bhav_files\\')
#getCSVFile(12,2014,30,'D:/Paarth/Google_Drive/Google Drive/Codes/Python/Work/Predict_StockMarket/data/bhav/','bhav')
#getMultipleBhav(12,2014,30,4,'D:/Paarth/Google_Drive/Google Drive/Codes/Python/Work/Predict_StockMarket/data/bhav/')

def getDayNameStr(P_Date):
    #print "Hi i am get Disply Str i will take date as input and give result as for 12th oct 2013 , Saturday 12 October 2013"
    Date=P_Date


def download_csv_load_sqlite(startDate='20131018',endDate='20131021',csv_dir='',dbDir='',dbName=''):
    print "Hi welcome to download_csv_load_sqlite function of SMA_download_csv_load_sqlite_by_date_ranges_v_1_0 module"
    print "Parameters passed to me are : "
    print "start date : "+startDate
    print "End date   : "+endDate
    print "Csv file dir: "+csv_dir
    print "DB Dir      : "+dbDir
    print "DB file Name : "+dbName

    #date entered into Int
    startDateInt=int(startDate)
    endDateInt=int(endDate)

    #i as start date in int format
    i=intToDateYMD(startDateInt)

    #Date till we run the loop
    oneDayAfterEndDate=dateAfterDays(endDateInt,1)

    listofDaysSuccessfullyDownloaded = []
    listofDaysFailsDownload = []
    #loop which runs from start date will end date
    while i != oneDayAfterEndDate:
        print i
        dateInNiceFormat=getDisplayStr(str(i))
        yearInt=int(str(i)[:4])
        monthInt=int(str(i)[4:-2])
        monthAbbr=calendar.month_abbr[monthInt].upper()
        dateInt=int(str(i)[-2:])
        print "\n\n****************************** "+dateInNiceFormat+" ****************************************"
        #Downloads csv files for date given from nseindia.com
        resultCsvDownload=getCSVFile(monthInt,yearInt,dateInt,csv_dir)
        print "result of CSV Download is ",resultCsvDownload
        #filename that s created by CSV


        if resultCsvDownload == 0:
            csvFileName=str(yearInt)+monthAbbr+str(dateInt)+".csv"
            resultCsvDbLoad=d.Bhavcsv_To_Sqlite3db(dbDir,dbName,csv_dir,csvFileName)
            print "File for "+dateInNiceFormat+" is successfully downloaded"
            print"Result of running Bhavcsv_To_Sqlite3db is ",resultCsvDbLoad
            TDate=dateBVformat(i)
            print " Result of Processing Bhav to D_BV is : "
            print d.processBhavD_BV(dbDir,dbName,TDate)
            listofDaysSuccessfullyDownloaded.append(dateInNiceFormat)

        else:
            dayName=getDayNameStr(str(i))
            listofDaysFailsDownload.append((dayName))
            if dayName in ('Saturday','Sunday'):
                print "File for "+dateInNiceFormat+" .Failed downloaded Reason Code : "+str(resultCsvDownload)+" Probable Reason : It was a Weekend "
            else:
                print "File for "+dateInNiceFormat+" .Failed downloaded Reason Code : "+str(resultCsvDownload)+" Probable Reason : Market might be closed today"
        i = dateAfterDays(i,1)



    """ Summary """

    print "\nFiles are successfully downloaded for days : "
    for val in listofDaysSuccessfullyDownloaded:
        print val

    print "\nFile Download Failed for  : "
    for val in listofDaysFailsDownload:
        print val

"""
print "Executing Create Bhav Table \n"
resultCreateTable=d.executeDDL(cnf.dbPath,cnf.dbName,cnf.createBhavTableSQL)
print "\nExecuting Create D Bhav Table\n"
resultCreateDTable=d.executeDDL(cnf.dbPath,cnf.dbName,cnf.create_D_BV)
bhavCSVPath=cnf.bhavCSVPath
DBPath=cnf.dbPath
DBName=cnf.dbName
print "\nExecuting downloadcsv and load into sqlite\n"
download_csv_load_sqlite(startDate='20131018',endDate='20131021',csv_dir=bhavCSVPath,dbDir=DBPath,dbName=DBName)
"""
bhavCSVPath=cnf.bhavCSVPath
DBPath=cnf.dbPath
DBName=cnf.dbName
download_csv_load_sqlite(startDate='20131018',endDate='20131021',csv_dir=bhavCSVPath,dbDir=DBPath,dbName=DBName)