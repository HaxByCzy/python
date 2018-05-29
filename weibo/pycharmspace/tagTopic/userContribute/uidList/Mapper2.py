#-*- coding:utf-8 _*-  
""" 
--------------------------------------------------------------------
@function: 计算话题用户贡献度列表
@time: 2018-01-28 
author:baoquan3 
@version: 
@modify: 
--------------------------------------------------------------------
"""
import sys

defaultencoding = 'utf-8'
if sys.getdefaultencoding() != defaultencoding:
    reload(sys)
    sys.setdefaultencoding(defaultencoding)


class UserContributeUidList(object):
    """
    计算某话题用户贡献度列表
    """
    def __init__(self):
        self.topicDict = {}     # 缓存话题的top-n用户列表

    def getContribute(self, line):
        """
        根据每行内容计算话题，对应用户的贡献度
        :param line:
        :return:(topic, uid, score)
        """
        lineArr = line.split("\t")
        if len(lineArr) == 2:
            uid, topic = "", ""
            keyArr = lineArr[0].split(",,,,")
            if len(keyArr) == 2:
                uid, topic = keyArr[0], keyArr[1]

            valArr = lineArr[1].split("||")
            typeDict = {}
            for elem in valArr:
                elemArr = elem.split(":")
                if len(elemArr) == 2:
                    typeDict[elemArr[0]] = int(elemArr[1])
            score = 0
            if "a" in typeDict:
                score += 5 * typeDict["a"]
            if "b" in typeDict:
                score += 1 * typeDict["b"]
            if "c" in typeDict:
                if typeDict["c"] > 0:
                    score += typeDict["c"] / 10
            if uid and topic:
                return (uid, topic, score)
            else:
                return None
        else:
            return None

    def run(self, line):
        """
        整体逻辑功能汇总
        :param line:
        :return:
        """
        ucTuple = self.getContribute(line)
        if ucTuple:
            uid, topic, score = ucTuple
            val = "{0}:{1}".format(score, uid)
            if topic in self.topicDict:
                tmpList = self.topicDict[topic]
                position = self.findInsertPosition(tmpList, val)
                tmpList.insert(position, val)
                if len(tmpList) >= 500:
                    tmpList = tmpList[:100]
                self.topicDict[topic] = tmpList

            else:
                self.topicDict[topic] = [val]
            if len(self.topicDict) >= 10000:
                self.output()

    def findInsertPosition(self,uidList, val):
        """
        二分法寻找列表插入位置
        :return:
        """
        low , high = 0, len(uidList)
        if high == 0:
            return low
        valScore = int(val.split(":")[0])
        while low < high:
            mid = (low + high) / 2
            midValScore = int(uidList[mid].split(":")[0])
            if valScore > midValScore:
                high = mid
            else:
                low = mid + 1
        return low


    def output(self):
        """
        对topicDict缓存话题进行过滤输出
        :return:
        """
        for topic, uidList in self.topicDict.iteritems():
            listSize = len(uidList)
            outLine = ""
            if listSize > 100:
                outList = uidList[:100]
                outLine = ",".join(outList)
            else:
                outLine = ",".join(uidList)
            sys.stdout.write("{0}\t{1}\n".format(topic, outLine))
        self.topicDict = {}


    def flush(self):
        if self.topicDict:
            for topic, uidList in self.topicDict.iteritems():
                listSize = len(uidList)
                outLine = ""
                if listSize > 100:
                    outList = uidList[:100]
                    outLine = ",".join(outList)
                else:
                    outLine = ",".join(uidList)
                if topic.strip():
                    sys.stdout.write("{0}\t{1}\n".format(topic, outLine))
            self.topicDict = {}


if __name__ == "__main__":
    ucui = UserContributeUidList()
    # for line in open("d://data//ucListIn.dat", "r"):
    for line in sys.stdin:
        if line.strip():
            ucui.run(line.strip())
    ucui.flush()
