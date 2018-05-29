#-*- coding:utf-8 _*-  
""" 
--------------------------------------------------------------------
@function: 话题养号 mapper 端处理
@time: 2017-08-03 
author:baoquan3 
@version: 
@modify: 
--------------------------------------------------------------------
"""
import sys
import json
sys.path.append("./util")
from util.Parser import Parser

defaultencoding = 'utf-8'
if sys.getdefaultencoding() != defaultencoding:
    reload(sys)
    sys.setdefaultencoding(defaultencoding)

class TopicUserMapper(Parser):
    """
    话题养号 mapper端处理
    """

    def processOneWeibo(self, fieldMap):
        """
        重写父类方法，接收微博，map 端输出处理
        :param fieldMap:
        :return:
        """
        action = fieldMap["ACTION"] if "ACTION" in fieldMap else ""
        filter = fieldMap["FILTER"] if "FILTER" in fieldMap else ""
        content = fieldMap["CONTENT"] if "CONTENT" in fieldMap else ""
        userinfo = fieldMap["INNER_USER_INFO"] if "INNER_USER_INFO" in fieldMap else ""
        uid = fieldMap["UID"] if "UID" in fieldMap else ""
        time = fieldMap["TIME"] if "TIME" in fieldMap else ""
        likenum = fieldMap["LIKENUM"] if "LIKENUM" in fieldMap else ""
        whitelikenum = fieldMap["WHITELIKENUM"] if "WHITELIKENUM" in fieldMap else ""

        if len(action) == 0 or action != "A" or len(filter) == 0 or len(content) == 0 or len(userinfo) == 0 or len(uid) == 0 or len(time) == 0:
            return None
        else:
            like = 0
            whitelike = 0.0
            validlike = 0
            if len(likenum) == 0 or len(whitelikenum) == 0:
                validlike = 0                   #有效点赞数
            else:
                like = int(likenum)
                whitelike = float(whitelikenum)
                if like <= 20 and whitelike == 0:
                    validlike = 0
                else:
                    validlike = int(like * (whitelike - 1) / 999)
            #用户基本信息解析
            user = json.loads(userinfo, encoding='utf8')["users"][0]
            if len(user) > 0:
                vfans = str(user["valid_fans"]) if "valid_fans" in user else ""
                ulevel = str(user["level"]) if "level" in user else ""
                fans = str(user["fans"]) if "fans" in user else ""
                follows = str(user["follows"]) if "follows" in user else ""
                if len(vfans) != 0 and len(ulevel) != 0 and len(fans) != 0 and len(follows) !=  0:
                    outLine = "{uid}\t{vfans}\t{ulevel}\t{content}\t{time}\t{fans}\t{follows}\t{filter}\t{validlike}".format(uid = uid, vfans = vfans, ulevel = ulevel,content = content, time = time, fans = fans, follows = follows,  filter = filter, validlike = validlike)
                    sys.stdout.write(outLine + "\n")

    def map(self, line):
        """
        map 处理每一行
        :param line:
        :return: None
        """
        self.processOneLine(line)

if __name__ == "__main__":
    tum = TopicUserMapper()
    for line in sys.stdin:
        tum.map(line.strip())
    tum.flush()
