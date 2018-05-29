#-*- coding:utf-8 _*-  
""" 
--------------------------------------------------------------------
@function: 复写继仁C++程序，给微博用户打分
@time: 2017-07-05 
author:baoquan3 
@version: 
@modify: 
--------------------------------------------------------------------
"""
import sys
from math import log
from math import exp
from math import pow
from setting import *

defaultencoding = 'utf-8'
if sys.getdefaultencoding() != defaultencoding:
    reload(sys)
    sys.setdefaultencoding(defaultencoding)

class UserScore(object):

    authTypeDict = {  "CELEBRITY": 0,
                      "GOVERNMENT": 1,
                      "ENTERPRISE": 2,
                      "MEDIA": 3,
                      "CAMPUS": 4,
                      "WEBSITE": 5,
                      "APPLICATION": 6,
                      "GROUP": 7,
                      "PENDING_BUSINESS": 8,
                      "PRIMARY_GOTTALENT": 200,
                      "THROUGH_GOTTALENT": 220,
                      "LATE_AUTHUSER": 400,
                      "ORDINARY_USER": -1}
    jsonOutTag = False

    def __init__(self):
        self.weightDict = {}

    def calculateUserScore(self, wb):
        """
        微博用户打分输出接口
        :param wb: 微博特征对象
        :return: 用户分
        """
        score = 1.0

        relationWeight = self.relationWeight(wb)
        inforWeight = self.inforWeight(wb)
        hotWeight = self.hotWeight(wb)
        typeWeight = self.typeWeight(wb)

        if wb.isVip:
            if relationWeight > 1:
                score *= relationWeight
            if hotWeight > 1:
                score *= hotWeight
            score *= inforWeight * typeWeight
        else:
            score *= relationWeight * hotWeight * inforWeight * typeWeight
        score = 68.0 / (1 + pow(3, -1.1 * score)) - 34

        score = float("%.4f" % score)
        if UserScore.jsonOutTag:
            self.weightDict["userScore"] = score
            self.weightDict["hitState"] = wb.user_state
        return 31 if score > 31 else score

    def relationWeight(self, wb):
        """
        计算用户的关系权重
        :param wb: 微博特征对象
        :return:  关系权重
        """
        relationWeight = {}
        weight = 1.0
        power = log(float(wb.validFansNum + 1), 10)
        param1 = log(float(power) + userparameter_2, userparameter_3) / userparameter_4 + userparameter_5
        param2 = 1.0 / (userparameter_6 + (exp(float(-userparameter_7) * (power - userparameter_8))))
        param3 = float(userparameter_9) / (1.0 + exp(float(-userparameter_10) * (float(wb.validFansRatio) - userparameter_11)))
        weight = param1 * param2 * param3

        minNum = wb.fansNum if wb.fansNum < wb.followsNum else wb.followsNum
        if wb.reputationRatio < 0.4 and wb.biFollowsNum < minNum / 3:
            weight -= userparameter_12
        elif wb.reputationRatio > 0.6 and wb.biFollowsNum > minNum / 2:
            weight += userparameter_12

        relationWeight["power"] = power
        relationWeight["param1"] = param1
        relationWeight["param2"] = param2
        relationWeight["param3"] = param3
        relationWeight["weight"] = weight
        self.weightDict["relationWeight"] = relationWeight
        return weight

    def inforWeight(self, wb):
        """
        计算用户的属性分
        :param wb:微博特征对象
        :return: 信息权重
        """
        weight = 1.0
        if wb.userRank != 0:
            weight += log(float(wb.userRank), 50) / 10 - userparameter_13

        self.weightDict["inforWeight"] = weight
        return weight

    def hotWeight(self, wb):
        """
        计算用户的热度
        :param wb: 微博特征对象
        :return: 热度分
        """
        hotWeight = {}
        weight = 1.0
        avgHot = hotparameter_0 * float(wb.avgForward) + hotparameter_1 * float(wb.avgLike)
        power1 = log(float(avgHot + 1), hotparameter_2)
        power2 = log(float(wb.validFansNum + 1), 10)
        param1 = (1.0 / (1 + exp(float(-hotparameter_3) * (power1 - hotparameter_4))) + hotparameter_5) / (1.0 + exp(float(-hotparameter_6) * (power1 + hotparameter_7)))
        param2 = float(hotparameter_8) / (1 + exp(hotparameter_9 * (power2 - hotparameter_10))) + hotparameter_11
        param3 = float(hotparameter_12) / (1 + exp(float(-hotparameter_13) * power1)) + hotparameter_14
        weight *= param1 * max(param2, param3)

        #	param_1 = (1/(1+e^(-0.6(x-4.4)))+1)*(1/(1+e^(-1.8(x+0.5)))); new
        #	param_2 = 0.1/(1+e^(1.2(x-5)))+0.9;
        #	param_3 = 0.4/(1+e^(-0.8x))+0.6;

        if UserScore.jsonOutTag:
            hotWeight["avgHot"] = avgHot
            hotWeight["power1"] = power1
            hotWeight["power2"] = power2
            hotWeight["param1"] = param1
            hotWeight["param2"] = param2
            hotWeight["param3"] = param3
            hotWeight["weight"] = weight
            self.weightDict["hotWight"] = hotWeight
        return weight

    def typeWeight(self, wb):
        """
        计算用户类型分
        :param wb: 微博特征对象
        :return: 类型分
        """
        weight = 1.0

        hitState = 0
        bitFlag = 1
        if wb.isVip:                            #vip用户
            hitState = bitFlag << 0
            weight = userparameter_0
            return weight
        elif wb.isBlack:                        #黑名单用户
            hitState = bitFlag << 1
            weight = userparameter_1
            return weight
        #用户级别
        if wb.userLevel == 1:
            hitState = bitFlag << 2
            weight *= userparameter_14
        elif wb.userLevel == 2:
            hitState = bitFlag << 3
            weight *= userparameter_15
        elif wb.userLevel == 3:
            hitState = bitFlag << 4
            weight *= userparameter_16
        elif wb.userLevel == 4:
            hitState = bitFlag << 5
            weight *= userparameter_17
        elif wb.userLevel == 5:
            hitState = bitFlag << 6
            weight *= userparameter_18

        weight_ori = weight

        if wb.authenticationType == UserScore.authTypeDict["CELEBRITY"]:                #名人黄V
            hitState |= bitFlag << 7
            weight *= userparameter_19
        elif wb.authenticationType == UserScore.authTypeDict["GOVERNMENT"]:             #政府蓝V
            hitState |= bitFlag << 8
            weight *= userparameter_20
        elif wb.authenticationType == UserScore.authTypeDict["ENTERPRISE"]:             #企业蓝V
            hitState |= bitFlag << 9
            weight *= userparameter_21
        elif wb.authenticationType == UserScore.authTypeDict["MEDIA"]:                  #媒体蓝V
            hitState |= bitFlag << 10
            weight *= userparameter_22
        elif wb.authenticationType == UserScore.authTypeDict["CAMPUS"]:                 #校园蓝V
            hitState |= bitFlag << 11
            weight *= userparameter_23
        elif wb.authenticationType == UserScore.authTypeDict["WEBSITE"]:                #网站蓝V
            hitState |= bitFlag << 12
            weight *= userparameter_24
        elif wb.authenticationType == UserScore.authTypeDict["APPLICATION"]:            #应用蓝V
            hitState |= bitFlag << 13
            weight *= userparameter_25
        elif wb.authenticationType == UserScore.authTypeDict["GROUP"]:                   #团体蓝V
            hitState |= bitFlag << 14
            weight *= userparameter_26
        elif wb.authenticationType == UserScore.authTypeDict["PENDING_BUSINESS"]:      #待审企业
            hitState |= bitFlag << 15
            weight *= userparameter_27
        elif wb.authenticationType == UserScore.authTypeDict["PRIMARY_GOTTALENT"]:     #初级达人
            hitState = bitFlag << 16
            weight *= userparameter_28
        elif wb.authenticationType == UserScore.authTypeDict["THROUGH_GOTTALENT"]:     #通过审核达人
            hitState |= bitFlag << 17
            weight *= userparameter_29
        elif wb.authenticationType == UserScore.authTypeDict["LATE_AUTHUSER"]:          #已故认证用户
            hitState |= bitFlag << 18
            weight *= userparameter_30
        elif wb.authenticationType == UserScore.authTypeDict["ORDINARY_USER"]:          #普通用户
            hitState |= bitFlag << 19
            weight *= userparameter_31

        if (hitState & 0x7F00) != 0 and wb.validFansNum < userparameter_47:                #命中蓝V且粉丝数较少
            coef = weight / weight_ori - 0.3
            weight = weight_ori * max(1.0, coef)

        if wb.userType & 1 << 1 != 0:                       #广告用户
            hitState |= bitFlag << 20
            weight *= userparameter_32
        elif wb.userUType & 1 << 6 != 0:                    #内容广告用户
            hitState |= bitFlag << 21
            weight *= userparameter_33
        elif wb.userUType & 1 << 1 != 0:                     #昵称简介广告用户
            hitState |= bitFlag << 22
            weight *= userparameter_34
        elif wb.userUType & 1 << 5 != 0:                     #昵称简介联系方式
            hitState |= bitFlag << 23
            weight *= userparameter_35
        elif wb.userType & 1 << 5 != 0:                      #联系电话用户
            hitState |= bitFlag << 24
            weight *= userparameter_36

        if wb.userUType & 1 << 2 != 0:                       #色情用户
            hitState |= bitFlag << 25
            weight *= userparameter_37
        if wb.userUType & 1 << 4 != 0:                       #作弊用户
            hitState |= bitFlag << 26
            weight *= userparameter_38
        if wb.userUType & 1 << 7 != 0:                       #营销用户
            hitState |= bitFlag << 27
            weight *= userparameter_39
        if wb.userUType & 1 << 9 != 0:                       #粉丝用户
            hitState |= bitFlag << 28
            weight *= userparameter_40
        if wb.userUType & 1 << 10 != 0:                       #沉寂用户
            hitState |= bitFlag << 29
            weight *= userparameter_41
        if wb.userUType & 1 << 11 != 0:                       #流失用户
            hitState |= bitFlag << 30
            weight *= userparameter_42
        if wb.userUType & 1 << 12 != 0:                       #话题养号用户
            hitState |= bitFlag << 31
            weight *= userparameter_43
        if wb.userType & 1 << 2 != 0:                         #头像用户
            hitState |= bitFlag << 32
            weight *= userparameter_44
        if wb.userType & 1 << 3 != 0:                         #机器用户
            hitState |= bitFlag << 33
            weight *= userparameter_45
        if wb.userType & 1 << 4 != 0:                         #头像用户
            hitState |= bitFlag << 34
            weight *= userparameter_46
        if wb.authenticationType == UserScore.authTypeDict["CELEBRITY"]:
            avgHotTotal = float(wb.avgForward) + float(wb.avgLike)
            param = float(hotparameter_15) / (1 + exp(float(-hotparameter_16) * (avgHotTotal - hotparameter_17))) + 1.0
            if param > 1.01:
                hitState |= bitFlag << 35
            weight *= param
        wb.user_state = hitState
        if UserScore.jsonOutTag:
            self.weightDict["typeWeight"] = weight
        return weight

    def getWeightJson(self):
        """
        将各权重得么以json形式返回，方便查看对比
        :return:
        """
        import json
        return json.dumps(self.weightDict)

