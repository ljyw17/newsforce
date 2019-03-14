#encoding= 'utf-8'
from pygraph.classes.digraph import digraph
from math import sqrt
import pymysql

class HITSIterator:
    __doc__ = '''计算一张图中的hub,authority值'''

    def __init__(self, dg):
        self.max_iterations = 100  # 最大迭代次数
        self.min_delta = 0.0001  # 确定迭代是否结束的参数
        self.graph = dg

        self.hub = {}
        self.authority = {}
        for node in self.graph.nodes():
            self.hub[node] = 1
            self.authority[node] = 1

    def hits(self):
        """
        计算每个页面的hub,authority值
        :return:
        """
        if not self.graph:
            return

        flag = False
        for i in range(self.max_iterations):
            change = 0.0  # 记录每轮的变化值
            norm = 0  # 标准化系数
            tmp = {}
            # 计算每个页面的authority值
            tmp = self.authority.copy()
            for node in self.graph.nodes():
                self.authority[node] = 0
                for incident_page in self.graph.incidents(node):  # 遍历所有“入射”的页面
                    self.authority[node] += self.hub[incident_page]
                norm += pow(self.authority[node], 2)
            # 标准化
            norm = sqrt(norm)
            for node in self.graph.nodes():
                self.authority[node] /= norm
                change += abs(tmp[node] - self.authority[node])

            # 计算每个页面的hub值
            norm = 0
            tmp = self.hub.copy()
            for node in self.graph.nodes():
                self.hub[node] = 0
                for neighbor_page in self.graph.neighbors(node):  # 遍历所有“出射”的页面
                    self.hub[node] += self.authority[neighbor_page]
                norm += pow(self.hub[node], 2)
            # 标准化
            norm = sqrt(norm)
            for node in self.graph.nodes():
                self.hub[node] /= norm
                change += abs(tmp[node] - self.hub[node])

            print("This is NO.%s iteration" % (i + 1))
            print("authority", self.authority)
            print("hub", self.hub)

            if change < self.min_delta:
                flag = True
                break
        if flag:
            print("finished in %s iterations!" % (i + 1))
        else:
            print("finished out of 100 iterations!")

        print("The best authority page: ", max(self.authority.items(), key=lambda x: x[1]))
        print("The best hub page: ", max(self.hub.items(), key=lambda x: x[1]))
        return self

if __name__ == '__main__':
    dg = digraph()

    sql = "select id, tranfromid from news"
    db = pymysql.connect("localhost", "root", "******", "world")
    try:
        cursor = db.cursor()
        cursor.execute(sql)
        results =cursor.fetchall()
        print ("开始写入数据")
        #开始添加图的点和边
        for row in results:
            id = row[0]
            dg.add_node(id)
        for row in results:
            id = row[0]
            tranfromid = row[1]
            if(tranfromid != None):
                dg.add_edge((id,tranfromid))
        print("写入完毕")
        print(dg)
    except:
        print("Error: unable to fetch data")

    # dg.add_nodes(["A", "B", "C", "D", "E", "F", "G"])
    #
    # dg.add_edge(("A", "E"))
    # dg.add_edge(("B", "E"))
    # dg.add_edge(("C", "E"))
    # dg.add_edge(("D", "G"))
    # dg.add_edge(("F", "E"))
    # print(dg)

    hits = HITSIterator(dg)

    print("开始处理数据")
    result = hits.hits()
    print(result.authority)

    print("结果开始写入数据库")
    for key,value in result.authority.items():
        updatesql = "update news set tranrate = " + str(value) + " where id = " + str(key)
        try:
            cursor.execute(updatesql)#更新
            db.commit()
        except:
            db.rollback()
        print(key,value)