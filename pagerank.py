from pygraph.classes.digraph import digraph
import pymysql

class PRIterator:
    __doc__ = '''计算一张图中的PR值'''

    def __init__(self, dg):
        self.damping_factor = 0.85  # 阻尼系数,即α
        self.max_iterations = 100  # 最大迭代次数
        self.min_delta = 0.00001  # 确定迭代是否结束的参数,即ϵ
        self.graph = dg

    def page_rank(self):
        #  先将图中没有出链的节点改为对所有节点都有出链
        for node in self.graph.nodes():
            if len(self.graph.neighbors(node)) == 0:
                for node2 in self.graph.nodes():
                    digraph.add_edge(self.graph, (node, node2))

        nodes = self.graph.nodes()
        graph_size = len(nodes)

        if graph_size == 0:
            return {}
        page_rank = dict.fromkeys(nodes, 1.0 / graph_size)  # 给每个节点赋予初始的PR值
        damping_value = (1.0 - self.damping_factor) / graph_size  # 公式中的(1−α)/N部分

        flag = False
        for i in range(self.max_iterations):
            change = 0
            for node in nodes:
                rank = 0
                for incident_page in self.graph.incidents(node):  # 遍历所有“入射”的页面
                    rank += self.damping_factor * (page_rank[incident_page] / len(self.graph.neighbors(incident_page)))
                rank += damping_value
                change += abs(page_rank[node] - rank)  # 绝对值
                page_rank[node] = rank

            print("This is NO.%s iteration" % (i + 1))
            print(page_rank)

            if change < self.min_delta:
                flag = True
                break
        if flag:
            print("finished in %s iterations!" % (i + 1))
        else:
            print("finished out of 100 iterations!")
        return page_rank


if __name__ == '__main__':
    dg = digraph()

    sql = "select id,tranfromid from news"
    db = pymysql.connect("localhost", "root", "******", "world")
    try:
        cursor = db.cursor()
        cursor.execute(sql)
        results =cursor.fetchall()
        print("开始写入数据")
        #添加点和边
        for row in results:
            id = row[0]
            dg.add_node(id)
        for row in results:
            id = row[0]
            tranfromid = row[1]
            if(tranfromid != None):
                dg.add_edge((id,tranfromid))
        print("写入数据完毕")
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

    pr = PRIterator(dg)
    print("开始执行处理")
    page_ranks = pr.page_rank()
    print("处理完毕，开始写入数据库")
    for key,value in page_ranks.items():
        updatesql = "update news set tranratepr = " + str(value) + " where id = " + str(key)
        try:
            cursor.execute(updatesql)#更新
            db.commit()
        except:
            db.rollback()
        print(key,value)

    # print("The final page rank is\n", page_ranks)