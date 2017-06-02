# -*- coding: utf-8 -*-

import sys
defaultencoding = 'utf-8'
if sys.getdefaultencoding() != defaultencoding:
    reload(sys)
    sys.setdefaultencoding(defaultencoding)
import MySQLdb
from MySQLdb.cursors import DictCursor
import time

"""
计算漫画付费信息
"""

tablePrefix = "pay_order_"
host = "10.9.98.143"
port = 3309
user = "user_read_pay"
passwd = "vwgvqefr57"
db = "kpayorder"

def comicPay():
    # 数据源链接
    beginDate = "2017-04-29 00:00:00"
    endDate = "2017-05-02 00:00:00"

    beginSec = str(getSecond(beginDate))
    endSec = str(getSecond(endDate))

    con = MySQLdb.connect(host=host, port=port, user=user, passwd=passwd, db=db, charset='utf8', cursorclass=DictCursor)
    cur = con.cursor()
    userPayDict = {}
    users = 0
    money = 0
    coins = 0
    for i in range(0, 128):
        index = "%03d" % i
        tableName = tablePrefix + index
        sql = "select count(DISTINCT(uid)) as userNum, sum(recharge_value) as coin, sum(pay_fee) as fee from " + tableName + " where pay_status = 2 and created_at >= " + beginSec + " and created_at < " + endSec
        cur.execute(sql)
        res = cur.fetchall()
        for row in res:
            userNum = row["userNum"]
            fee = row["fee"]
            coin = row["coin"]
            users += userNum
            money += fee
            coins += coin
            print users,userNum,"----",money, fee,"-----",coins, coin, tableName, sql
    print users, money / 100 , coins


def getSecond(date):
    time.strptime(date,'%Y-%m-%d %H:%M:%S')
    second = int(time.mktime(time.strptime(date,'%Y-%m-%d %H:%M:%S')) * 1000)
    print date, second
    return second



def timeProcess():
    date = 1493417528199
    oldDate = time.localtime(int(date)/1000)
    newDate = time.strftime('%Y-%m-%d %H:%M:%S',oldDate)
    #print newDate
    tmp = "2017-04-29 06:12:08"
    time.strptime(tmp,'%Y-%m-%d %H:%M:%S')
    #print int(time.mktime(time.strptime(tmp,'%Y-%m-%d %H:%M:%S')) * 1000)
    #print date

    beginDate = "2017-04-29 00:00:00"
    endDate = "2017-05-02 00:00:00"

    beginSec = str(getSecond(beginDate))
    endSec = str(getSecond(endDate))




if __name__ ==  "__main__":
    outputFile = "D://data//gamePay.dat"
    # outputFile = sys.argv[2]
    #coinUseNum( outputFile)

    timeProcess()

    #comicPay()
