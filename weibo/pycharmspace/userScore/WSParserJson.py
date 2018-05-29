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
from UserScore import  UserScore


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
        self.isAuthenUser = False    #是否认证用户
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

class WeiboParserJson(object):

    authenticationType = "vtype"             #认证类型

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


    def processOneWeibo(self, line):
        """
        根据传入的微博内容，解析json数据
        :param line:文本内容
        :return: 加入特征的微博bean
        """
        lineArr = line.split("\t")
        wb = Weibo()
        if len(lineArr) == 2:
            uid = lineArr[0].strip()
            wb.uid = uid
            featureDict = json.loads(lineArr[1].strip(), encoding="utf8")
            #认证类型
            if WeiboParserJson.authenticationType in featureDict:
                wb.authenticationType = featureDict[WeiboParserJson.authenticationType]
            #粉丝量
            if WeiboParserJson.fansNum in featureDict:
                wb.fansNum = featureDict[WeiboParserJson.fansNum]
            #有效粉丝量
            if WeiboParserJson.validFansNum in featureDict:
                wb.validFansNum = featureDict[WeiboParserJson.validFansNum]
            if wb.validFansNum > wb.fansNum:
                wb.validFansNum = wb.fansNum
            #有效粉丝率
            if wb.fansNum != 0:
                wb.validFansRatio = float(wb.validFansNum) / wb.fansNum
            else:
                wb.validFansRatio = 0.0
            #关注数
            if WeiboParserJson.followsNum in featureDict:
                wb.followsNum = featureDict[WeiboParserJson.followsNum]
            #声誉率
            if wb.fansNum != 0 or wb.followsNum != 0:
                wb.reputationRatio = float(wb.fansNum) / (wb.fansNum + wb.followsNum)
            else:
                wb.reputationRatio = 0.0
            #互粉数
            if WeiboParserJson.biFollowsNum in featureDict:
                wb.biFollowsNum = featureDict[WeiboParserJson.biFollowsNum]
            #微博等级
            if WeiboParserJson.userRank in featureDict:
                wb.userRank = featureDict[WeiboParserJson.userRank]
            #用户级别
            if WeiboParserJson.userLevel in featureDict:
                wb.userLevel = featureDict[WeiboParserJson.userLevel]
            #用户UType、vip、 black
            if WeiboParserJson.userUType in featureDict:
                wb.userUType = featureDict[WeiboParserJson.userUType]
                if (wb.userUType & (1 << 3)) != 0:
                    wb.isVip = False
                    wb.isBlack = True
                elif (wb.userUType & (1 << 8)) != 0:
                    wb.isVip = True
                    wb.isBlack = False
                else:
                    wb.isVip = False
                    wb.isBlack = False
            #用户type
            if WeiboParserJson.userType in featureDict:
                wb.userType = featureDict[WeiboParserJson.userType]
            #平均热度
            if WeiboParserJson.averageHot in featureDict:
                avghotDict = featureDict[WeiboParserJson.averageHot]
                if WeiboParserJson.avgForward in avghotDict:
                    wb.avgForward = avghotDict[WeiboParserJson.avgForward]
                if WeiboParserJson.avgLike in avghotDict:
                    wb.avgLike = avghotDict[WeiboParserJson.avgLike]
        return wb







