#encoding ='utf-8'
import pymysql

#计算回复率
def getconrate(numcon):
    if(numcon <= 500):
        return 0.05
    elif(numcon <= 1000):
        return 0.1
    elif(numcon <= 1500):
        return 0.2
    elif(numcon <= 2000):
        return 0.3
    elif(numcon <= 2500):
        return 0.4
    elif(numcon <= 3000):
        return 0.5
    elif(numcon <= 3500):
        return 0.6
    elif(numcon <= 4000):
        return 0.7
    elif(numcon <= 4500):
        return 0.8
    elif(numcon <= 5000):
        return 0.9
    else:
        return 1

if __name__ == '__main__':
    db = pymysql.connect('localhost','root','******','world')
    cursor = db.cursor()
    #只计算源新闻的回复率
    selectsql = 'select id, numcon from news where (tranfromid is null or conment != numcon)'
    try:
        cursor.execute(selectsql)
        result = cursor.fetchall()
        for row in result:
            conrate = getconrate(row[1])#得到回复率
            updatesql = 'update news set conmentrate = ' + str(conrate) + ' where id = ' + str(row[0])
            print("{0}的回复率是{1}".format(row[0], conrate))
            try:
                cursor.execute(updatesql)#更新
                db.commit()
            except:
                db.rollback()
    except:
        print("Error: unable to fetch data")