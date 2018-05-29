#-*- coding:utf-8 _*-  
""" 
--------------------------------------------------------------------
@function: hdfs数据get到本地前，根据 HIT_BASE过滤
@time: 2017-09-04
author:baoquan3
@version:
@modify:
--------------------------------------------------------------------
"""
import sys
import time
import json
sys.path.append("./util")
from Parser import Parser
defaultencoding = 'utf-8'
if sys.getdefaultencoding() != defaultencoding:
    reload(sys)
    sys.setdefaultencoding(defaultencoding)

QI_MOVE = ((1 << 7) | (1 << 8) | (1 << 22) | (1 << 9))
QI_CLEAR = ((1 << 1) | (1 << 4) | (1 << 5) | (1 << 6) | (1 << 9) | (1 << 12) | (1 << 13) | (1 << 19))
UT_CLEAR = ((1 << 6) | (1 << 7) | (1 << 4) | (1 << 3) | (1 << 2))

class QiFormat(Parser):
    """
    质量分入索引库格式转换
    """

    def transform(self, bak_qi, qi, ut, score):
        user_notwhite = qi & (1 << 7)
        dict_1 = qi & (1 << 8)
        user_machine = qi & (1 << 22)
        cheat_domain = qi & (1 << 9)

        new_qi = qi & ~QI_MOVE
        new_qi &= ~QI_CLEAR;
        new_ut = ut & ~UT_CLEAR;

        if user_machine : new_ut |= (1 << 7)
        if user_notwhite : new_qi |= (1 << 1)
        if dict_1 : new_qi |= (1 << 22)
        if cheat_domain : new_qi |= (1 << 12)

        new_qi |= (score & 63) << 4
        new_qi |= (1 << 13)

        if bak_qi == 1042:
            new_qi = 0;
            new_qi |= (1 << 2) | (1 << 17) | (1 << 20) | (1 << 19)
        return (new_qi, new_ut)

    def processOneWeibo(self, fieldMap):
        outDict = {}

        # 邮件中未给出
        if "ID" in fieldMap and fieldMap["ID"] != "":
            outDict["ID"] = fieldMap["ID"]
        else:
            return None
        outDict["ACTION"] = "M"
        tmp_time = time.localtime((int(outDict["ID"]) >> 22) + 515483463)
        dt = time.strftime("%Y-%m-%d %H:%M:%S",tmp_time)
        outDict["TIME"] = dt

        # 时间
        if "INNER_USER_INFO" in fieldMap:
            if outDict["TIME"] < "2014-19-01 00:00:00":
                outDict["INNER_USER_INFO"] = fieldMap["INNER_USER_INFO"]

        # 获取质量分，为qi 和 user_type转换，准备数据
        score = 0
        if "QUALITY_SCORE" in fieldMap and fieldMap["QUALITY_SCORE"] != "":
            score = self.getScore1(json.loads(fieldMap["QUALITY_SCORE"], encoding='utf8'))
        elif "QI" in fieldMap and fieldMap["QI"] != "":
            score = self.getScore2(fieldMap["QI"])
        userTypeOld = int(fieldMap["USER_TYPE_OLD"]) if "USER_TYPE_OLD" in fieldMap and fieldMap["USER_TYPE_OLD"] != "" else 0
        userTypeOut = -1

        # QI 格式处理
        qi = None
        qi_bak = 0
        if "QI_BAK" in fieldMap or "QI_OLD" in fieldMap:
            if "QI_BAK" in fieldMap and fieldMap["QI_BAK"] != "":
                qi_bak = int(fieldMap["QI_BAK"])
                qi = qi_bak
            elif "QI_OLD" in fieldMap and fieldMap["QI_OLD"] != "":
                qi = int(fieldMap["QI_OLD"])
            # 质量分转换
            if qi != None:
                qi, userTypeOut = self.transform(qi_bak, qi, userTypeOld, score)
        elif "QI" in fieldMap and fieldMap["QI"] != "":
            qi = int(fieldMap["QI"])
        elif "QI_NEW" in fieldMap and fieldMap["QI_NEW"] != "":
                qi = int(fieldMap["QI_NEW"])
        if qi != None:
            qi = qi | (1 << 13)
            outDict["QI"] = qi

        # userType 处理
        if "USER_TYPE_OLD" in fieldMap and fieldMap["USER_TYPE_OLD"] != "":
            if userTypeOut != -1:
                outDict["USER_TYPE"] = userTypeOut
            else:
                outDict["USER_TYPE"] = 0
        else:
            if "USER_TYPE" in fieldMap and fieldMap["USER_TYPE"] != "":
                # 2,3 位清0
                outDict["USER_TYPE"] = (int(fieldMap["USER_TYPE"]) & 0xFFFFFFF3)

        # 邮件中给出
        if "QI_BAK" in fieldMap:
            outDict["QI_BAK"] = fieldMap["QI_BAK"]
        if "QI_OLD" in fieldMap:
            outDict["QI_OLD"] = fieldMap["QI_OLD"]
        if "USER_TYPE_OLD" in fieldMap:
            outDict["USER_TYPE_OLD"] = fieldMap["USER_TYPE_OLD"]
        if "TERMSWEIGHT" in fieldMap:
            outDict["TERMSWEIGHT"] = fieldMap["TERMSWEIGHT"]
        if "TW_USERTEXT" in fieldMap:
            outDict["TW_USERTEXT"] = fieldMap["TW_USERTEXT"]
        if "CONT_SIGN" in fieldMap:
            outDict["CONT_SIGN"] = fieldMap["CONT_SIGN"]
        if "CONT_SIGN_OLD" in fieldMap:
            outDict["CONT_SIGN_OLD"] = fieldMap["CONT_SIGN_OLD"]
        if "DUP_CONT" in fieldMap:
            outDict["DUP_CONT"] = fieldMap["DUP_CONT"]
        if "DUP_CONT_OLD" in fieldMap:
            outDict["DUP_CONT_OLD"] = fieldMap["DUP_CONT_OLD"]
        if "QUALITY_SCORE" in fieldMap:
            outDict["QUALITY_SCORE"] = fieldMap["QUALITY_SCORE"]
        if "QUALITY_RANK" in fieldMap:
            outDict["QUALITY_RANK"] = fieldMap["QUALITY_RANK"]


        # 输出
        sys.stdout.write("@\n")
        for key, val in outDict.iteritems():
            sys.stdout.write("@" + str(key) + ":" + str(val) + "\n")


    def getScore1(self,qualityScoreDict):
        """
         通过qualityScore字段获取f_score值
        :param qualityScoreDict:
        :return:
        """
        score = 0
        if "f_score" in qualityScoreDict:
            score = qualityScoreDict["f_score"]
        return int(score)

    def getScore2(self,QI):
        """
        获取QI的4至9位
        :param QI:
        :return:
        """
        # 取数值的 4 - 9 位数字， 0x03F0 : 0000 0011 1111 0000
        qi_4_9 = (int(QI) & 0x03F0) >> 4
        return int(qi_4_9)


if __name__ == "__main__":
    hbf = QiFormat()
    for line in sys.stdin:
        hbf.processOneLine(line.strip())
    hbf.flush()


