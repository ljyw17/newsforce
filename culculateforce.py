#encoding ='utf-8'
import pymysql

#计算影响力
def get_force(timeparameter,webweight,tranrate,conmentrate,a,b):
    return timeparameter * webweight * (a * tranrate + b * conmentrate)

if __name__ == '__main__':
    a = 0.8
    b = 0.2
    db = pymysql.connect('localhost','root','******','world')
    cursor = db.cursor()
    #只计算源新闻的影响力
    selectsql = 'select id, timeparameter, webweight, tranratepr, conmentrate from news where (tranfromid is null or conment != numcon)'
    try:
        cursor.execute(selectsql)
        results = cursor.fetchall()

        for row in results:
            force = get_force(row[1], row[2], row[3], row[4], a, b)
            updatesql = 'update news set newsforce = ' + str(force) + ' where id = ' + str(row[0])
            print("{0}的新闻影响力是{1}".format(row[0], force))
            try:
                cursor.execute(updatesql)#更新
                db.commit()
            except:
                db.rollback()
    except:
        print("Error: unable to fetch data")