#-*- coding:utf-8 _*-  
""" 
--------------------------------------------------------------------
@function: 索引有效性微博格式变换
@time: 2017-08-30 
author:baoquan3 
@version: 
@modify: 
--------------------------------------------------------------------
"""
import sys
sys.path.append("./util")
from Parser import Parser
import json
defaultencoding = 'utf-8'
if sys.getdefaultencoding() != defaultencoding:
    reload(sys)
    sys.setdefaultencoding(defaultencoding)


class Mapper(Parser):
    """
    抽取用户的某些字段信息
    """

    def processOneWeibo(self, fieldMap):
        uid = fieldMap["UID"] if "UID" in fieldMap else "null"
        userInfor = fieldMap["INNER_USER_INFO"] if "INNER_USER_INFO" in fieldMap else "null"
        url = fieldMap["URL"] if "URL" in fieldMap else "null"
        content = fieldMap["CONTENT"] if "CONTENT" in fieldMap else "null"
        text = fieldMap["TEXT"] if "TEXT" in fieldMap else "null"
        time = fieldMap["TIME"] if "TIME" in fieldMap else "null"
        level = int(fieldMap["LEVEL"]) if "LEVEL" in fieldMap else 1
        rtmid = fieldMap["RTMID"] if "RTMID" in fieldMap else "0"
        bi_follows, fans, follows, valid_fans, rank = 0, 0, 0, 0, 0
        if userInfor != "null":
            try:
                userJson = json.loads(userInfor, encoding='utf8')["users"][0]
            except BaseException:
                sys.stderr.write("reporter:counter:weibo,userInfoJsonErr,1\n")
                return None
            if "bi_follows" in userJson:
                bi_follows = userJson["bi_follows"]
            if "fans" in userJson:
                fans = userJson["fans"]
            if "valid_fans" in userJson:
                valid_fans = userJson["valid_fans"]
            if "urank" in userJson:
                rank = userJson["urank"]
        userFeature = {"bi_follows": bi_follows, "fans" : fans, "valid_fans" : valid_fans, "level": level, "rank" : rank}
        if rtmid != "0":
            userFeature["rtmid"] = rtmid
        outJson = json.dumps(userFeature)
        sys.stdout.write("{0}\t{1}\t{2}\t{3}\t{4}\t{5}\n".format(uid, time, outJson, url, content, text))


if __name__ == "__main__":
    map = Mapper()
    for line in sys.stdin:
        map.processOneLine(line.strip())
    map.flush()
