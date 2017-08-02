# -*- coding: UTF-8 -*-
import urllib2
import sys  
import chardet
import pymysql
from bs4 import BeautifulSoup
import time
import random

urlBase = '''http://117.169.20.240:9090'''  # www.dy2018.com
currentBaseUrl = ''

# 设置系统编码为utf8
reload(sys)
sys.setdefaultencoding('utf8')
# 不同的电影类型
listtype =['0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20']

# 将Python连接到MySQL中的python数据库shcema中
conn = pymysql.connect(user="root", password="root", database="python", charset='utf8')
cur = conn.cursor()
# 如果数据库中有dy2018allmovies的数据库则删除
cur.execute('DROP TABLE IF EXISTS dy2018allmovies')
sql = """CREATE TABLE dy2018allmovies(
            title CHAR(255) NOT NULL,
            score CHAR(255),
            author CHAR(255),
            time CHAR(255),
            publish CHAR(255),
            person CHAR(255),
            type CHAR(255),
            tag CHAR(255),
            url CHAR(255)
     )"""

cur.execute(sql)  # 执行sql语句，新建一个dy2018allmovies的数据库
i = 0  # 下载数量
j = 0  # 出错数量
start = time.clock()
for types in listtype:

    # 循环获取电影类型
    # 随机睡一会 防止爬的太快被拉黑
    time.sleep(int(format(random.randint(0, 9))))

    currentBaseUrl = urlBase+'/' + types
    # 伪装浏览器头
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.78 Safari/537.36'}
    req = urllib2.Request(url=currentBaseUrl, headers=headers)
    res = urllib2.urlopen(req)
    content = res.read()
    typeEncode = sys.getfilesystemencoding()  # 系统默认编码
    infoencode = chardet.detect(content).get('encoding')  # 通过第3方模块来自动提取网页的编码
    htmlBase = content.decode(infoencode, 'ignore').encode(typeEncode)  # 先转换成unicode编码，然后转换系统编码输出
    soupBase = BeautifulSoup(htmlBase, "lxml")

    # 当前类型电影的所有页面
    listpages = soupBase.select("html body div#header div.contain div.bd2 div.bd3 div.bd3r div.co_area2 div.co_content8 div.x p select")


    # 获取当前类型电影的不同页码url
    for v_page_url in listpages[0]:
        try:
            # 当前页码的网站地址
            currentUrl = urlBase+v_page_url.get('value')
            time.sleep(int(format(random.randint(0, 3))))
            # 处理当前页的电影
            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.78 Safari/537.36'}
            req = urllib2.Request(url=currentUrl, headers=headers)
            res = urllib2.urlopen(req)
            content = res.read()
            typeEncode = sys.getfilesystemencoding()  # 系统默认编码
            infoencode = chardet.detect(content).get('encoding')  # 通过第3方模块来自动提取网页的编码
            html = content.decode(infoencode, 'ignore').encode(typeEncode)  # 先转换成unicode编码，然后转换系统编码输出
            soup = BeautifulSoup(html, "lxml")

            # 当前页所有电影的信息
            # 标题
            listtitle = soup.select("#header > div > div.bd2 > div.bd3 > div.bd3r > div.co_area2 > div.co_content8 > ul > td > table > tr:nth-of-type(2) > td > b > a:nth-of-type(2)")
            # 评分
            listscore = soup.select("#header > div > div.bd2 > div.bd3 > div.bd3r > div.co_area2 > div.co_content8 > ul > td > table > tr:nth-of-type(3) > td > font:nth-of-type(2)")
            # 电影类型
            typelist = soup.select("html body div#header div.contain div.bd2 div.bd3 div.bd3r div.co_area2 div.title_all h1 font")
            # 电影信息 如名称  导演 等  这个为什么遍历超卡************************************
            # ***************因为路径中间多了个空格
            # *************************************************************************
            # 注意：listmessage获取时发现超级卡，循环一次消耗时间几秒  最后找到原因：soup.select()的路径参数中某两个节点间多了一个空格(之前的用的是firefox里的css路径，中间是以空格隔开)，后来换成"空格>空格"方式
            # 获取节点路径不知有没有好的靠谱的方法???????????????????
            listmessage = soup.select("#header > div.contain > div.bd2 > div.bd3 > div.bd3r > div.co_area2 > div.co_content8 > ul > td > table > tr:nth-of-type(4) > td > p:nth-of-type(1)")
            currentType = typelist[0].get_text().replace('电影天堂', '').replace('>', '').strip()
            for v_title, v_score, v_message in zip(listtitle, listscore, listmessage):
                i=i+1
                print v_title.get_text() +'    '+v_score.get_text()
                message = v_message.get_text().split("\r\n")
                l = []  # 建一个列表，用于存放标题，分数，时间等数据
                try:
                    title = message[0].replace('◎片名:','').strip()
                    score = v_score.get_text().replace('◎评分: ', '').strip()
                    author = message[2].replace('◎导演:','').strip()
                    opertime = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())  # 操作时间
                    publish = title[0:4]  # 年份
                    person = ''
                    types = currentType
                    tag = v_title.get('title')
                    url = urlBase + v_title.get('href')  # 电影地址
                except IndexError:
                        continue
                l.append([title, score, author, opertime, publish, person,types, tag,url])

                # 这是一条sql插入语句
                sql = "INSERT INTO dy2018allmovies values(%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                # 执行sql语句，并用executemary()函数批量插入数据库中
                cur.executemany(sql, l)
                conn.commit()

                print '提交第'+str(i)+'条电影记录 : '+url + '    ' + opertime
        except:
            j = j + 1
            print '下载出错：第'+str(j)+'条'
            continue


#  mysql数据库用完了关掉
if cur:
        cur.close()
if conn:
        conn.close()


end = time.clock()


print('总共耗时:', end - start, '秒')    # 爬取结束，输出爬取时间
print '总共下载成功 ' + str(i) + '条'
print '总共下载失败 ' + str(j) + '条'