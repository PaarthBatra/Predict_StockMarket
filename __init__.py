__author__ = 'pbatra'

def dateToBhavTimestamp(P_Date):
    #given 20150105 i will return 05-JAN-2015
    StrDate=str(P_Date)
    #print "dateToBhavTimestamp : Value passed = %s ",StrDate
    D=StrDate[6:]
    #print D
    Y=StrDate[:4]
    #print Y
    M=StrDate[4:][:2]
    #print "M value is ",M
    if M == "01":
        M="JAN"
    elif M == "02":
        M="FEB"
    elif M == "03":
        M="MAR"
    elif M == "04":
        M="APR"
    elif M == "05":
        M="MAY"
    elif M == "06":
        M="JUN"
    elif M == "07":
        M="JUL"
    elif M == "08":
        M="AUG"
    elif M == "09":
        M="SEP"
    elif M == "10":
        M="OCT"
    elif M == "11":
        M="NOV"
    elif M == "12":
        M="DEC"
    else:
        print "I should never print . Track me at download_bhav --> dateToBhavTimestamp"


    #print "M is ",M
    return D + '-' + M + '-' + Y