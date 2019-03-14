# -*-coding:utf-8-*-

import jieba
import jieba.analyse
import numpy as np
import pymysql

# hash操作
def string_hash(source):
    if source == "":
        return 0
    else:
        x = ord(source[0]) << 7
        m = 1000003
        mask = 2 ** 128 - 1
        for c in source:
            x = ((x * m) ^ ord(c)) & mask
        x ^= len(source)
        if x == -1:
            x = -2
        x = bin(x).replace('0b', '').zfill(64)[-64:]
        # print(source, x)  # 打印 （关键词，hash值）
        return str(x)

# 海明距离计算
def hammingDis(simhash1, simhash2):
    t1 = '0b' + simhash1
    t2 = '0b' + simhash2
    n = int(t1, 2) ^ int(t2, 2)
    i = 0
    while n:
        n &= (n - 1)
        i += 1
    return i

#得到simhash编码
def getsimhash(line):
    seg = jieba.cut(line)  # 分词
    # jieba.analyse.set_stop_words('stopword.txt')  # 去除停用词
    keyWord = jieba.analyse.extract_tags(
        '|'.join(seg), topK=45, withWeight=True, allowPOS=())  # 先按照权重排序，再按照词排序
    keyList = []
    flagseg = list(jieba.posseg.cut(line))
    for feature, weight in keyWord:  # 对关键词进行hash
        for w in flagseg:
            # print(w.word, w.flag)
            if (str(w.word) == feature and (w.flag == 'n' or w.flag == 'nr' or w.flag == 'nr1' or w.flag == 'nr2' or w.flag == 'nrj' or w.flag == 'nrf' or w.flag == 'ns' or w.flag == 'nsf' or w.flag == 'nt' or w.flag == 'nz' or w.flag == 'nl' or w.flag == 'ng')):
                weight = weight * 5#命名实体加权
                break
        #     elif(str(w.word) == feature and (w.flag == 'v' or w.flag == 'vd' or w.flag == 'vn' or w.flag == 'vi' or w.flag == 'vl' or w.flag == 'vg')):
        #         weight = weight * 3
        #         break
        feature = string_hash(feature)
        temp = []
        for i in feature:
            if (i == '1'):
                temp.append(weight)
            else:
                temp.append(-weight)
        # print (temp)  # 将hash值用权值替代
        keyList.append(temp)
    list_sum = np.sum(np.array(keyList), axis=0)  # 45个权值列向相加
    # print ('list_sum:', list_sum)  # 权值列向求和
    if (keyList == []):  # 编码读不出来
        print ('00')
    simhash = ''
    for i in list_sum:  # 权值转换成hash值
        if (i > 0):
            simhash = simhash + '1'
        else:
            simhash = simhash + '0'
    return simhash

if __name__ == "__main__":#数据库操作过于杂乱繁琐，可以简化
    db = pymysql.connect("localhost","root","******","scrapyspider" )
    db2 = pymysql.connect("localhost","root","******","scrapyspider" )
    # 使用 cursor() 方法创建一个游标对象 cursor
    cursor = db.cursor()
    cursor2 = db2.cursor()
    # SQL 查询语句sql用来加载第一循环的数据，sql2用来加载第二次循环的数据
    sql = "SELECT id,content,releasetime,conment,tranfromid,newstype FROM news where id > 1852"
    sql2 = "SELECT id,content,releasetime,conment,tranfromid,numcom,newstype FROM news"
    try:
        # 执行SQL语句
        cursor.execute(sql)
        cursor2.execute(sql2)
        # 获取所有记录列表
        results = cursor.fetchall()
        results2 = cursor2.fetchall()
        i = 1848#用来计算第一循环的轮数
        for row in results:#第一个循环
            i += 1
            id = int(row[0])
            print("----------------------------------------------")
            print("正在处理id为",id,"的新闻")
            content = str(row[1])#内容用来比较相似度
            releasetime = row[2]#用来对两个新闻进行发布时间的先后进行比较
            conment = int(row[3])#与tranfromid2的评论数进行比较，确定tranfromid2更新与否
            tranfromid = row[4]#这个用来后续的比较之前判断出的源转载新闻的评论数和当前判断出的源转载新闻的评论数的大小关系
            newstype = str(row[5])#暂未用到
            j = 0#用来控制第二次循环中取新闻的起点
            for row2 in results2:#第二个循环，和第一个循环中的值进行比较
                j += 1
                if(j > i):#id2大于id时开始判断相似度
                    id2 = int(row2[0])
                    content2 = str(row2[1])
                    releasetime2 = row2[2]#用来对两个新闻进行发布时间的先后进行比较
                    conment2 = int(row2[3])#与tranfromid的评论数进行比较，确定tranfromid更新与否
                    tranfromid2 = row2[4]#这个用来后续的比较之前判断出的源转载新闻的评论数和当前判断出的源转载新闻的评论数的大小关系
                    numcom2 = int(row2[5])#暂未用到
                    newstype2 = str(row2[6])#暂未用到
                    if(int(id) != int(id2)):#如果不是同一条id新闻
                        dis = hammingDis(getsimhash(content), getsimhash(content2))#新闻相似度比较
                        print("%d和%d的相似度是dis = %d" % (id, id2, dis))

                        if(dis <= 13):#如果相似
                            if(releasetime.timestamp() - releasetime2.timestamp() >= 0):
                                print("%d转载了%d的新闻"%(int(id), int(id2)))#根据时间先后判断转载主客关系
                                db3 = pymysql.connect("localhost", "root", "******", "scrapyspider")#更新id的tranfromid
                                db5 = pymysql.connect("localhost", "root", "******", "scrapyspider")#查询id的评论数
                                db6 = pymysql.connect("localhost", "root", "******", "scrapyspider")#更新id2的numcom
                                cursor3 = db3.cursor()
                                cursor5 = db5.cursor()
                                cursor6 = db6.cursor()
                                if (tranfromid != None):#如果转载id已经有值
                                    db4 = pymysql.connect("localhost", "root", "******", "scrapyspider")#求出tranfromid的评论数
                                    cursor4 = db4.cursor()
                                    selectsql = "select conment from news where id = " + str(tranfromid)
                                    cursor4.execute(selectsql)
                                    conment4 = cursor4.fetchall()
                                    tranconment = int(conment4[0][0])#求出转载的源新闻的评论数
                                    db4.close()
                                    if(tranconment > conment2):#判断评论数，决定转载id
                                        id2 = int(tranfromid)
                                updatesql = "UPDATE news SET tranfromid = " + str(id2) + " WHERE id = " + str(id)
                                conmentsql = "select conment from news where id = " + str(id)
                                cursor5.execute(conmentsql)
                                conmenttonum = int(cursor5.fetchall()[0][0])#读取当前的评论数，用于后续的累加
                                updatesql2 = "UPDATE news SET numcom = numcom +" + str(conmenttonum) + " WHERE id = " + str(id2)
                                tranfromid = id2#更新id的转载id
                                try:
                                    # 执行SQL语句
                                    cursor3.execute(updatesql)#更新转载id
                                    try:
                                        cursor6.execute(updatesql2)#累加源新闻的评论数
                                        db6.commit()
                                    except:
                                        db5.rollback()
                                    # 提交到数据库执行
                                    db3.commit()
                                except:
                                    # 发生错误时回滚
                                    db3.rollback()
                                # 关闭数据库连接
                                db3.close()
                                db5.close()
                                db6.close()
                            else:
                                print("%d转载了%d的新闻" % (int(id2), int(id)))  # 根据时间先后判断转载主客关系
                                db3 = pymysql.connect("localhost", "root", "******","scrapyspider")  # 更新id2的tranfromid
                                db5 = pymysql.connect("localhost", "root", "******", "scrapyspider")  # 查询id2的评论数
                                db6 = pymysql.connect("localhost", "root", "******", "scrapyspider")  # 更新id的numcom
                                cursor3 = db3.cursor()
                                cursor5 = db5.cursor()
                                cursor6 = db6.cursor()
                                if (tranfromid2 != None):  # 如果转载id2已经有值
                                    db4 = pymysql.connect("localhost", "root", "******","scrapyspider")  # 求出tranfromid2的评论数
                                    cursor4 = db4.cursor()
                                    selectsql = "select conment from news where id = " + str(tranfromid2)
                                    cursor4.execute(selectsql)
                                    conment4 = cursor4.fetchall()
                                    tranconment = int(conment4[0][0])#求出转载的源新闻的评论数
                                    db4.close()
                                    if (tranconment > conment):  # 判断评论数，决定转载id
                                        id = int(tranfromid2)
                                updatesql = "UPDATE news SET tranfromid = " + str(id) + " WHERE id = " + str(id2)
                                conmentsql = "select conment from news where id = " + str(id2)
                                cursor5.execute(conmentsql)
                                conmenttonum = int(cursor5.fetchall()[0][0])#读取当前的评论数，用于后续的累加
                                updatesql2 = "UPDATE news SET numcom = numcom +" + str(conmenttonum) + " WHERE id = " + str(id)
                                tranfromid2 = id  # 更新id的转载id
                                try:
                                    # 执行SQL语句
                                    cursor3.execute(updatesql)  # 更新转载id
                                    try:
                                        cursor6.execute(updatesql2)  # 累加源新闻的评论数
                                        db6.commit()
                                    except:
                                        db5.rollback()
                                    # 提交到数据库执行
                                    db3.commit()
                                except:
                                    # 发生错误时回滚
                                    db3.rollback()
                                # 关闭数据库连接
                                db3.close()
                                db5.close()
                                db6.close()


    except:
        print("Error: unable to fetch data")

    # 关闭数据库连接
    db.close()
    db2.close()

    # str1 = "铅笔道3月7日讯，Facebook平台上的美国用户正在流失，根据市场研究机构Edison Research的一份调查显示，自2017年来，该平台在美国失去了约1500万用户。这份调查并没有详细指出用户离开该平台的原因。该机构主席Larry Rosin称，这已经是连续第二年监测到Facebook平台上有美国用户流失的趋势，这1500万用户占据了全美12岁以上人口的6%。美国市场一直是Facebook最重要的市场，去年第四季度财报显示，Facebook全球月活跃用户数为23.2亿，其中美国和加拿大地区月活用户为2.42亿。去年第四季度，Facebook从北美地区获得的营收为84.33亿美元，占总营收的50%以上，是营收最高的地区，平均从每个北美用户身上获得的收入为34.86美元，为所有地区最高。Facebook平台上流失的用户，很多开始流入Instagram这款同样是Facebook旗下的社交应用。目前Instagram用户数已超过10亿，去年Instagram两位创始人先后退出，意味着Facebook开始加强对旗下Instagram的掌控。目前Instagram已经并入Facebook产品线下进行统一管理"
    # str_2 = "据marketplace消息，Facebook平台上的美国用户正在流失，市场研究机构Edison Research的一份调查显示，自2017年来，该平台在美国失去了约1500万用户。该机构主席Larry Rosin称，这已经是连续第二年监测到Facebook平台上美国用户流失的趋势，1500万用户占据了全美12岁以上人口的6％。Facebook平台上的美国用户正在流失，市场研究机构Edison Research的新数据显示，与2017年相比，美国的用户数量估计减少了1500万，最大的下降是在12至34岁的非常理想的群体中。该机构主席Larry Rosin称，这已经是连续第二年监测到Facebook平台上美国用户流失的趋势，1500万用户占据了全美12岁以上人口的6％，他认为这是一个非常显著的数字。据透露，美国近80％的人都在发帖，发推文或抢购，但Facebook上的人数较少。"
    # str2 = "驱动中国 2018年6月29日消息 此前，Facebook由于管理用户资料存在重大漏洞，导致5000万用户数据泄露事件在社交媒体上引发轩然大波。该事件不仅导致Facebook股价急跌，更掀起一轮删除Facebook的浪潮。今日，Facebook 数据丑闻再被曝出。t0117c1161557502047_副本据外媒报道，道德黑客、漏洞赏金猎人Inti De Ceukelaire周四披露，Nametests.com的第三方智力竞赛应用让1.2亿Facebook用户的数据面临泄露风险。而这个应用的漏洞直到上个月才得到修复，这就使得Facebook数据丑闻再次被曝光在公众眼前。据De Ceukelaire发现，只要用户注册Nametests.com网站中的任何一款智力竞赛应用，他们在Facebook上的个人数据都会被泄漏。这些数据包括姓名、出生日期、婚姻状态、好友名单、图片等等。即便是用户删除了这些应用，这些数据也依然会被泄漏。由此看来，Facebook的问题或远不止被曝光的这些。这也恰好证明了许多专家先前的怀疑：剑桥分析只是冰山一角。RIGf-heqpwqy5488956_副本众所周知，这已经不是Facebook首次陷入数据丑闻风波了，今年3月份Facebook隐私权丑闻被首次曝光，当时曾在唐纳德.特朗普(Donald 　Trump)竞选美国总统期间受聘的政治数据公司“剑桥分析”(Cambridge 　Analytica)被曝从一名教授那里非法购买了Facebook用户数据，该教授运作过一个名为 “thisisyourdigitallife” (这是你的数字生活)的测验应用。Facebook首席技术官迈克-斯克洛普夫(Mike 　Schroepfer)后来证实，此次数据泄露事件最终涉及到8700万Facebook用户，且主要为美国用户。此次，Facebook 再度被曝数据丑闻，势必会再次引发众怒。目前，Facebook 一系列侵犯用户隐私事件，已遭到世界各地公民的强烈抗议，Facebook 未来的发展前途将被蒙上阴影。"
    # str1 = "中新网3月6日电 综合报道，英国伦敦警方发布消息指出，伦敦火车站和2个机场收到自制爆炸装置，警方将这起事件视作恐怖主义事件，已展开调查。消息称：“伦敦警方反恐部门5日在英国首都3处公共设施收到3个可疑包裹后启动调查。这些包裹分别装在白色邮袋里，内有黄色物体，经专家研究，确定是小型爆炸装置。”伦敦警察局局长助理、英国资深反恐专家马克·罗利表示，警方目前正在进一步调查袭击者的个人资料和社交关系，重点调查他们发动袭击是否得到其他人的协助。图为一名警察在被搜查的房子外。资料图：伦敦警察。其中一个爆炸装置已在希思罗机场附近的一幢办公楼内爆炸，引发一场小型火灾。机场没有受到影响，没有航班被取消，但作为预防措施，当局疏散了办公大楼。没有人在爆炸中受伤。一个多小时后，警方接到一个电话，说又有一个可疑包裹寄到了滑铁卢火车站。当局做出回应，并确保了装置安全。大约30分钟后，他们接到了第三个包裹的电话，这个包裹被送到了伦敦城市机场附近的一座办公楼。伦敦警察厅表示，他们将这起事件视为恐怖主义事件。据《独立报》报道，爆炸装置被装在包裹里，包裹上有爱尔兰邮票。没有引爆的两个包裹似乎是同一个人用同一支笔亲手写的地址。英国天空新闻台证实，爱尔兰警方正在协助伦敦警察厅进行调查。有关官员正在继续调查这一事件，并警告人们注意类似的包裹。"
    # str_2 = "综合报道，英国伦敦警方发布消息指出，伦敦火车站和2个机场收到自制爆炸装置，警方将这起事件视作恐怖主义事件，已展开调查。机场火车站接连收到可疑包裹消息称：“伦敦警方反恐部门5日在英国首都3处公共设施收到3个可疑包裹后启动调查。这些包裹分别装在白色邮袋里，内有黄色物体，经专家研究，确定是小型爆炸装置。”其中一个爆炸装置已在希思罗机场附近的一幢办公楼内爆炸，引发一场小型火灾。机场没有受到影响，没有航班被取消，但作为预防措施，当局疏散了办公大楼。没有人在爆炸中受伤。一个多小时后，警方接到一个电话，说又有一个可疑包裹寄到了滑铁卢火车站。当局做出回应，并确保了装置安全。大约30分钟后，他们接到了第三个包裹的电话，这个包裹被送到了伦敦城市机场附近的一座办公楼。警方视为恐袭事件 展开调查伦敦警察厅表示，他们将这起事件视为恐怖主义事件。据《独立报》报道，爆炸装置被装在包裹里，包裹上有爱尔兰邮票。没有引爆的两个包裹似乎是同一个人用同一支笔亲手写的地址。英国天空新闻台证实，爱尔兰警方正在协助伦敦警察厅进行调查。有关官员正在继续调查这一事件，并警告人们注意类似的包裹。"
    # str2 = "埃及首都开罗老城区2月18日晚发生一起爆炸，造成两名警察和一名恐怖分子死亡。根据埃及内政部的声明，爆炸地点位于开罗市中心著名的爱资哈尔清真寺附近。发生爆炸时，两名警察正在追捕一名恐怖分子，恐怖分子被抓获后引爆了藏匿在身上的炸弹。爆炸还造成3名警察受伤。埃及内政部的声明称，这起爆炸事件与15日吉萨省发生的一起未遂爆炸案有关。据报道，埃及安全部队15日在吉萨省拆除了一个自制简易爆炸装置，该爆炸装置的袭击目标是一座清真寺附近的检查站。埃及国家信息服务中心提供的数字显示，2018年埃及境内的爆炸事件为8起，而2017年为50起，2016年199起，2014年多达222起。进入2019年，盘踞在埃及境内的恐怖分子频频制造事端，致使该国安全状况再度变得严峻。这一爆炸事件为埃及安全形势再次敲响警钟。埃及政府果断采取措施，不断加大反恐力度。开罗爆炸事件发生后，埃及政府召开紧急会议，决定继续对恐怖分子采取“铁腕行动”。埃及内政部19日发表声明称，安全部队在北西奈省阿里什市确认了两处恐怖分子据点，随后在突袭行动中打死16名恐怖分子，并缴获若干武器、弹药及爆炸装置。2018年2月，埃及军方开始实施“西奈2018”反恐行动，截至当年年底，军方在行动中打死约450名恐怖分子，逮捕了近千名恐怖分子或嫌犯，成功摧毁了恐怖组织的据点、隧道、武器库等。这些恐怖分子属于与伊斯兰国有关联的激进组织，其主要头目和军事骨干被击毙。除了在西奈半岛展开大规模的军事行动外，埃及还在全国各地对潜在的恐怖活动强化预警机制。此次爆炸事件后，埃及进一步加强了对政府机构、重要旅游景点、基督教堂、清真寺等的安保力度。自2017年4月埃及北部城市坦塔和亚历山大分别发生针对教堂的恐怖袭击后，埃及全国一直处于“紧急状态”中。埃及总统塞西多次签发总统令，延长紧急状态，以加大对恐怖分子的威慑力度。埃及官方发布的公报强调说：“延长紧急状态，可以允许军队和警察采取必要措施应对恐怖分子威胁，断绝恐怖分子的资金来源，以维护国家安全与公民人身和财产安全”。"
    # str1 = "新京报快讯 联想集团今日（7日）通过微信公众号发声：3月5日，自媒体号“数码e起谈”在自媒体平台首发题为《联想杨元庆再惹争议，宁愿放弃5G也不选华为，高通比华为强太多》的造谣文章，随后大批自媒体进行了有组织的二次洗稿发布，数百个军事乃至美妆博主集中在微博平台转发诽谤，造成了极其恶劣的社会影响。联想集团就此严正声明如下：　　联想集团董事长兼CEO杨元庆先生从未在任何场合发表过类似言论。我们已完成取证工作，将正式起诉相关造谣自媒体，通过法律手段维护自身权益。　　最近一年，此类无中生有的谣言以极高的频率不断出现。我们将采取一切合法手段调查长期恶意挑动中国公司内耗的幕后黑手，将不惜任何成本挖掘相关组织或个人恶意操控社会舆论行为的真实目的，将其绳之以法。　　同时，我们也呼吁所有中国企业珍惜中国制造近年来不断提升的国际形象，远离水军、黑公关等非常规手段，共同营造公平有序的品牌舆论环境，一起在国内和国际舞台上获得更大成就。"
    # str2 = "联想集团今日发布声明，就自媒体号“数码e起谈”在自媒体平台首发题为《联想杨元庆再惹争议，宁愿放弃5G也不选华为，高通比华为强太多》的文章做出回应，称联想集团董事长兼CEO杨元庆从未在任何场合发表过类似言论。联想集团称，已完成取证工作，将正式起诉相关造谣自媒体，通过法律手段维护自身权益。“最近一年，此类无中生有的谣言以极高的频率不断出现。我们将采取一切合法手段调查长期恶意挑动中国公司内耗的幕后黑手，将不惜任何成本挖掘相关组织或个人恶意操控社会舆论行为的真实目的，将其绳之以法。”联想集团称，呼吁所有中国企业珍惜中国制造近年来不断提升的国际形象，远离水军、黑公关等非常规手段，共同营造公平有序的品牌舆论环境，一起在国内和国际舞台上获得更大成就。据悉，《联想杨元庆再惹争议，宁愿放弃5G也不选华为，高通比华为强太多》的文章在网络形成快速转发，随后大批自媒体进行了有组织的二次洗稿发布，数百个军事乃至美妆博主集中在微博平台转发诽谤，联想集团认为，这造成了极其恶劣的社会影响。实际上，今年的巴塞罗那MWC大会上，雷帝网就在现场见证了杨元庆的这场采访。杨元庆接受雷帝网等媒体群访时表示，很多厂商在做PPT产品，发布了产品放在玻璃屏里。“我不知道是希望产品与用户隔得远一点，还是担心真实的用户体验被用户触摸到，能够买到的时间恐怕也在半年以后。”杨元庆说，有些折叠屏手机价格高高在上，达到2000美元以上，都可以买好几个PAD产品。不过，杨元庆在整个采访过程中并未提及到华为或者是三星，更没有说过宁愿放弃5G也不选华为，高通比华为强太多的话。"
    # str2 = "9月16日，联想集团发布声明针对日前外媒报道其董事长兼CEO杨元庆称联想集团不是一家中国公司言论做出澄清。联想集团在声明中说：该报道曲解了杨元庆当时完整的表述，并在标题中断章取义，进一步衍生出错误解读。杨元庆也同时发布微博称：“没想到我的一个外媒采访会引发一个小波澜，全球化过程中的lost in translation是一个长足的功课啊…我一直的梦想，联想不仅要做一家成功的中国公司，更要做一家具有包容力的全球化公司，因为我们要做全世界的生意，要吸引全世界的人才和资源。”联想集团的声明如下：今日，我们注意到联想集团董事长兼CEO杨元庆先生近期接受外媒采访时的一篇英文报道发生错误引用。该报道曲解了杨元庆当时完整的表述，并在标题中断章取义，进一步衍生出错误解读。在此，我们愿意作出澄清如下：9月13日，联想集团在美国纽约举办年度Lenovo Transform大会，与全球客户、合作伙伴及媒体沟通最新业务发展。大会期间，杨元庆先生接受系列外媒采访，并在回答公司战略与业务相关问题时表示：“联想不仅仅是中国公司，更是一家全球公司。联想的全球布局不仅在各大市场部署营销力量，我们在中国、美国、巴西和德国设计研发产品，我们在中国、美国、巴西和墨西哥生产制造这些产品。我们是一个真正意义上的全球公司。”9月26-27日，一年一度的“联想创新科技大会”即将在北京开幕，我们愿在联想集团这一旗舰行业盛会上，与媒体朋友及各行各业做更深入的沟通交流。联想集团植根中国，制胜全球，将全心全意为中国乃至全球的消费者与行业客户提供最好的用户体验，助力每一个人、每一个行业都能蓬勃发展，拥有智慧未来。"