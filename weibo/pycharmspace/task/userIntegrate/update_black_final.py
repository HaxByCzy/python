#-*- coding:utf-8 _*-  
""" 
--------------------------------------------------------------------
@function: 将数据写入redis
@time: 2018-02-11 
author:baoquan3 
@version: 
@modify: 
--------------------------------------------------------------------
"""
import sys
import redis
import time

defaultencoding = 'utf-8'
if sys.getdefaultencoding() != defaultencoding:
    reload(sys)
    sys.setdefaultencoding(defaultencoding)

class data2redis(object):

    def __init__(self, inputFile, logFile):
        """
        初始化数据库链接
        :return:
        """
        self.pool0 = redis.ConnectionPool(host = "pkm25135.eos.grid.sina.com.cn", port=25135 ,db = 0, decode_responses=True)
        self.pool1 = redis.ConnectionPool(host = "pkm25136.eos.grid.sina.com.cn", port=25136 ,db = 0, decode_responses=True)
        self.r0 = redis.Redis(connection_pool=self.pool0)
        self.r1 = redis.Redis(connection_pool=self.pool1)
        self.pipe0 = self.r0.pipeline(transaction=False)
        self.pipe1 = self.r1.pipeline(transaction=False)
        self.pipeGroup = [self.pipe0, self.pipe1]
        self.cacheDict, self.cacheNum , self.idSet = {}, 0, set([])
        self.inputFile, self.totalLineNum = inputFile, 0
        self.logFile = open(logFile, "w")

    def readLine(self):
        """
        读取文件
        :return:
        """
        with open(self.inputFile, "r") as inFile:
            for line in inFile:
                self.totalLineNum += 1
                yield line.strip()

    def run(self):
        """
        实现整体逻辑
        :param inFile:
        :return:
        """
        for line in self.readLine():
            lineArr = line.split("\t")

            # 缓存数据并预获取redis中原有值
            if len(lineArr) == 2:
                try:
                    uid, val = int(lineArr[0]), int(lineArr[1])
                    partitionId = uid / 10 % len(self.pipeGroup)
                    # 将数据缓存取字典中
                    self.idSet.add(partitionId)
                    if partitionId in self.cacheDict:
                        self.cacheDict[partitionId].append([uid, val])
                    else:
                        self.cacheDict[partitionId] = [[uid, val]]

                    # 获取redis中值
                    self.pipeGroup[partitionId].hget(uid, "14")
                    self.cacheNum += 1
                except BaseException:
                    sys.stderr.write("errLine : line \n")

            # 将数据缓存到列表,清空缓存数量
            if self.cacheNum >= 100:
                self.writeRedis()

                # 打印输出日志
                currentTime = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
                self.logFile.write("{0}--{1}--{2}--{3}\n".format(currentTime, self.totalLineNum, partitionId, line))
                self.logFile.flush()

    def writeRedis(self):
        """
        将数据写回redis
        :return:
        """
        # 获取用户在redis中值， 本地数据处理,并写回redis
        if self.cacheDict:
            # 缓存取到的数据
            for partitionId in self.idSet:
                pipe = self.pipeGroup[partitionId]
                result = pipe.execute()
                rawVal = self.cacheDict[partitionId]
                for i in range(0, len(result)):
                    uid, val = rawVal[i]
                    srcVal = int(result[i]) if result[i] else 0
                    if val == 1:
                        outVal = srcVal | 0x4000
                        pipe.hset(uid, "14", outVal)
                    elif val == 0:
                        outVal = srcVal | (~0x4000)
                        pipe.hset(uid, "14", outVal)

            # 将pipe中缓存写入的数据，执行写入redis
            for partitionId in self.idSet:
                pipe = self.pipeGroup[partitionId]
                pipe.execute()

            # 清空本地设置的缓存数据
            self.cacheNum = 0
            self.cacheDict = {}
            self.idSet = set([])

    def flush(self):
        """
        最后输出缓存数据
        :return:
        """
        self.writeRedis()
        self.logFile.write("successfully finished! \n")
        self.logFile.close()

if __name__ == "__main__":
    inputFile = sys.argv[1]
    logFile = sys.argv[2]
    d2r = data2redis(inputFile, logFile)
    # inputFile = "d://data//test.dat"
    d2r.run()
    d2r.flush()