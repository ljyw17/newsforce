from math import log
from math import pow
import pymysql
import math
import datetime

def getTimeParameter(time1,time2,newstype):
    den = 24
    if(newstype == "科技"):
        den = 72#非时事类新闻的时间周期不同

    if(time1 > time2):
        print("错误的时间大小")
    else:
        delta = (time2 - time1).total_seconds()/(60 * 60)#时间差转换成小时制
        alpha = -(1 / den) * log(0.5)
        return pow(math.e, -alpha * (delta))

if __name__ == "__main__":
    selectdb = pymysql.connect("localhost", "root", "******", "world")
    cursor = selectdb.cursor()
    #只计算源新闻的时间参数
    selectsql = "select id,releasetime,collecttime,newstype from news where (tranfromid is null or conment != numcon)"
    try:
        cursor.execute(selectsql)
        results = cursor.fetchall()
        for row in results:
            timeparameter = getTimeParameter(row[1],row[2],row[3])#计算时间参数
            updatedb = pymysql.connect("localhost", "root", "******", "world")
            cursor = updatedb.cursor()
            updatesql = "update news set timeparameter = " + str(timeparameter) + " where id = " + str(row[0])
            print("id为",row[0],"的新闻的时间参数是",timeparameter)
            try:
                cursor.execute(updatesql)
                updatedb.commit()
            except:
                updatedb.rollback()
    except:
        print("Error: unable to fetch data")