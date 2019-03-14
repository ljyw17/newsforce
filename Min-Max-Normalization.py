import numpy
import pymysql

listid = []
listforce = []
db = pymysql.connect('localhost', 'root', '******', 'world')
cursor = db.cursor()
selectsql = 'select id, newsforce from news where(tranfromid is null or conment != numcon)'
try:
    cursor.execute(selectsql)
    results = cursor.fetchall()
    for row in results:
        listid.append(row[0])
        listforce.append(row[1])
except:
    print('Error: unable to fetch data')

arrayforce = numpy.asarray(listforce)
i = 0#用于检索出id值
for x in arrayforce:
    #归一化计算
    x = float(x - numpy.min(arrayforce))/(numpy.max(arrayforce) - numpy.min(arrayforce))
    updatesql = 'update news set news_force_pr = ' + str(x) + ' where id = ' + str(listid[i])
    try:
        cursor.execute(updatesql)
        db.commit()
    except:
        db.rollback()

    print(listid[i],x)
    i += 1