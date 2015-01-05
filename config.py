__author__ = 'pbatra'
bhavDownloadURL="www.nseindia.com"
bhavCSVPath='D:/Paarth/Google_Drive/Google Drive/Codes/Python/Work/Predict_StockMarket/data/bhav/'
dbPath='D:/Paarth/Google_Drive/Google Drive/Codes/Python/Work/Predict_StockMarket/data/db/'
dbName='SMP.db'

#SQLs
createBhavTableSQL="""create table BHAV (SYMBOL TEXT,SERIES TEXT,OPEN integer,HIGH integer,LOW
                        integer,CLOSE	integer,LAST integer,PREVCLOSE integer,TOTTRDQTY integer,TOTTRDVAL integer,
                        TIMESTAMP text,TOTALTRADES integer,ISIN text,Primary key(SYMBOL,SERIES,TIMESTAMP));"""

create_D_BV="""create table D_BV(SYMBOL TEXT,SERIES TEXT,OPEN integer,HIGH integer,LOW
                        integer,CLOSE	integer,LAST integer,PREVCLOSE integer,TOTTRDQTY integer,TOTTRDVAL integer,
                        TIMESTAMP integer,TOTALTRADES integer,ISIN text,Primary key(SYMBOL,SERIES,TIMESTAMP));"""