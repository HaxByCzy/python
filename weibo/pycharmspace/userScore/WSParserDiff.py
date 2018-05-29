#-*- coding:utf-8 _*-  
"""
--------------------------------------------------------------------
@function: 对微博的特征进行解析
@time: 2017-07-04 
author:baoquan3 
@version: 
@modify:
--------------------------------------------------------------------
"""
import sys
import json
from UserScore import UserScore


defaultencoding = 'utf-8'
if sys.getdefaultencoding() != defaultencoding:
    reload(sys)
    sys.setdefaultencoding(defaultencoding)

class Weibo(object):

    def __init__(self):
        self.uid = 0                #微博作者
        self.fansNum = 0            #用户粉丝数
        self.validFansNum = 0       #有效粉丝数
        self.followsNum = 0         #用户关注数
        self.validFansRatio = 0.0   #有效粉丝数占比
        self.reputationRatio = 0.0  #粉丝/(粉丝+关注)
        self.biFollowsNum = 0       #互粉数
        self.isAuthenUser = False   #是否认证用户
        self.authenticationType = -1 #认证类型
        self.userRank = 0           #用户微博等级
        self.userLevel = 3          #用户级别
        self.userUType = 0          #用户类型utype
        self.userType = 0           #用户类型type
        self.isVip = False           #vip用户
        self.isBlack = False         #黑名单用户

        self.avgForward = 0.0        #平均有效转
        self.avgLike = 0.0           #平均有效赞

        self.user_state = 0         #用户命中状态

        self.userScore = 0.0        #C++用户分

class WeiboParser(object):

    recordSep = "@"                          #单条微博的开始标识符
    fieldSep = ":"                           #字段与值的分割符
    uid = "UID"                              #微博作者ID
    isAuthenUser = "VERIFIED"               #是否是认证用户
    authenticationType = "VERIFIEDTYPE"    #认证类型

    userInfor = "INNER_USER_INFO"          #用户属性信息
    fansNum = "fans"                        #用户粉丝数
    validFansNum = "valid_fans"            #有效粉丝数
    followsNum = "follows"                 #用户关注数
    biFollowsNum = "bi_follows"            #互粉数
    userRank = "urank"                      #用户微博等级
    userLevel = "level"                     #用户级别
    userUType = "utype"                     #用户类型utype
    userType = "type"                       #用户类型tpye

    averageHot = "aveHot"                   #平均热度
    avgForward = "vfwn"                     #平均有效转
    avgLike = "vlike"                       #平均有效赞

    def __init__(self):
        self.weiboList = [] #存放单个微博列表
        self.tmpDict = {}   #临时存放微博的原始字段值

    def processOneRecord(self, line):
        """
        单条微博特征解析
        :param line:
        :return:
        """
        if line is WeiboParser.recordSep:
            self.insertWeibo2List()
            self.tmpDict.clear()
        else:
            index = line.find(WeiboParser.fieldSep)
            if index != -1:
                fieldName = line[1: index].strip()
            if len(fieldName) > 0:
                val = line[index + 1 : len(line)]
                if len(val) > 0 :
                    self.tmpDict[fieldName] = val

    def insertWeibo2List(self):
        if len(self.tmpDict) > 0:
            wb = Weibo()
            if WeiboParser.uid in self.tmpDict:
                #作者ID
                wb.uid = int(self.tmpDict[WeiboParser.uid])
                #用户属性信息
                if WeiboParser.userInfor in self.tmpDict:
                    self.userTraitAnalysis(wb, self.tmpDict[WeiboParser.userInfor])
                #是否为认证用户
                if WeiboParser.isAuthenUser in self.tmpDict:
                    wb.isAuthenUser = False if self.tmpDict[WeiboParser.isAuthenUser] == "0" else True
                #认证类型
                if WeiboParser.authenticationType in self.tmpDict:
                    wb.authenticationType = int(self.tmpDict[WeiboParser.authenticationType])

                #解析C++用户评分
                if "QUALITY_SCORE" in self.tmpDict:
                    wb.userScore = float(json.loads(self.tmpDict["QUALITY_SCORE"],encoding = "utf8")["s_user"])

                #将实例化特征的微博对象加入到列表中
                self.weiboList.append(wb)

    def userTraitAnalysis(self,wb , userInfor):
        """
        解析用户属性json串
        :param wb:
        :param userInfor:
        :return:
        """
        userDict = json.loads(userInfor,encoding = "utf8")["users"][0]
        #粉丝数
        if WeiboParser.fansNum in userDict:
            wb.fansNum = userDict[WeiboParser.fansNum]
        #有效粉丝数，
        if WeiboParser.validFansNum in userDict:
            wb.validFansNum = userDict[WeiboParser.validFansNum]
        if wb.validFansNum > wb.fansNum:
            wb.validFansNum = wb.fansNum
        #有效粉丝占比
        if wb.fansNum != 0:
            wb.validFansRatio = float(wb.validFansNum) / wb.fansNum
        else:
            wb.validFansRatio = 0.0
        #关注数
        if WeiboParser.followsNum in userDict:
            wb.followsNum = userDict[WeiboParser.followsNum]
        #声誉率
        if wb.fansNum != 0 or wb.followsNum != 0:
            wb.reputationRatio = float(wb.fansNum) / (wb.fansNum + wb.followsNum)
        else:
            wb.reputationRatio = 0.0
        #互粉数
        if WeiboParser.biFollowsNum in userDict:
            wb.biFollowsNum = userDict[WeiboParser.biFollowsNum]
        #微博等级
        if WeiboParser.userRank in userDict:
            wb.userRank = userDict[WeiboParser.userRank]
        #用户级别
        if WeiboParser.userLevel in userDict:
            wb.userLevel = userDict[WeiboParser.userLevel]
        #用户类型uType
        if WeiboParser.userUType in userDict:
            wb.userUType = userDict[WeiboParser.userUType]
            if (wb.userUType & (1 << 3)) != 0:
                wb.isVip = False
                wb.isBlack = True
            elif (wb.userUType & (1 << 8)) != 0:
                wb.isVip = True
                wb.isBlack = False
            else:
                wb.isVip = False
                wb.isBlack = False
        #用户类型
        if WeiboParser.userType in userDict:
            wb.userType = userDict[WeiboParser.userType]
        #用户平均有效转、平均有效赞
        if WeiboParser.averageHot in userDict:
            avgHotDict = userDict[WeiboParser.averageHot]
            #平均有效转
            if WeiboParser.avgForward in avgHotDict:
                wb.avgForward = avgHotDict[WeiboParser.avgForward]
            #平均有效赞
            if WeiboParser.avgLike in avgHotDict:
                wb.avgLike = avgHotDict[WeiboParser.avgLike]

    def getWeiboList(self, filePath):
        """
        根据本地微博文件，返回各微博特征列表，方便调用数据
        :param filePath: 本地微博文件路径
        :return: 微博用户特征列表
        """
        with open(filePath, "r") as inFile:
            for line in inFile:
                line = line.strip()
                if len(line) > 0:
                    self.processOneRecord(line)
        self.flush()
        return self.weiboList

    def flush(self):
        """
        将最后解析的微博放入weiboList中
        :return:
        """
        self.insertWeibo2List()
        self.tmpDict.clear()


if __name__ == "__main__":
    inFile = "D://data//part.dat"
    wp = WeiboParser()
    us = UserScore()
    wbList = wp.getWeiboList(inFile)
    for wb in wbList:
        scoreSrc = us.calculateUserScore(wb)
        scorePy = float("%.2f" % us.calculateUserScore(wb))
        if wb.userScore == scorePy:
            print wb.uid,wb.userScore,scorePy,scoreSrc
        else:
            print '----',wb.uid,wb.userScore,scorePy,scoreSrc
            print "wb.fansNum = " + str(wb.fansNum) + ";"
            print "wb.validFansNum = " + str(wb.validFansNum) + ";"
            print  "wb.followsNum = " + str(wb.followsNum) + ";"
            print "wb.validFansRatio = " + str(wb.validFansRatio) + ";"
            print "wb.reputationRatio = " + str(wb.reputationRatio) + ";"
            print "wb.biFollowsNum = " + str(wb.biFollowsNum) + ";"
            print "wb.isAuthenUser = " + str(wb.isAuthenUser) + ";"
            print "wb.authenticationType = " + str(wb.authenticationType) + ";"
            print "wb.userRank = " + str(wb.userRank) + ";"
            print "wb.userLevel = " + str(wb.userLevel) + ";"
            print "wb.userUType = " + str(wb.userUType) + ";"
            print "wb.userType = " + str(wb.userType) + ";"
            print "wb.isVip = " + str(wb.isVip) + ";"
            print "wb.isBlack = " + str(wb.isBlack) + ";"
            print "wb.aveHot.aveForward = " + str(wb.avgForward) + ";"
            print "wb.aveHot.aveLike = " + str(wb.avgLike) + ";"
            print "-----------------------"
            print "relation : " + str(us.relationWeight(wb))
            print "infor : " + str(us.inforWeight(wb))
            print "hot : " + str(us.hotWeight(wb))
            print "type : " + str(us.typeWeight(wb))
            print "==============================================="



