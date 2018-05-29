#-*- coding:utf-8 _*-  
""" 
--------------------------------------------------------------------
@function: 按行解析微博流，该模块实现微博流的解析基类，使用装饰模式子类重写、添加新的功能
@time: 2017-08-03 
author:baoquan3 
@version: 0.1
@modify:
--------------------------------------------------------------------
"""
import sys

defaultencoding = 'utf-8'
if sys.getdefaultencoding() != defaultencoding:
    reload(sys)
    sys.setdefaultencoding(defaultencoding)

class Parser(object):
    """
    微博流解析基类，子类复写具体微博处理过程，或添加新功能
    """

    weiboSep = "@"              #单条微博的开始标识符
    fieldSep = ":"              #字段与值的分割符

    def __init__(self):
        self.__fieldMap = {}      #私有成员，微博各字段 kye value 存放字典
        self.__fieldKeyList = []  #私有成员，微博字段key序列

    def processOneLine(self, line):
        """
        解析微博的每一行，对外提供的调用接口
        :param line: 微博的一行内容
        :return: None
        """
        line = line.strip()
        if line:
            if line == Parser.weiboSep:
                if self.__fieldMap :
                    self.processOneWeibo(self.__fieldMap)
                    self.processOneWeiboModify(self.__fieldKeyList, self.__fieldMap)
                    self.__fieldKeyList = []
                    self.__fieldMap.clear()
            else:
                self.__fieldSplit(line)

    def processOneWeibo(self, fieldMap):
        """
        根据需要处理每条微博，该方法由子类重写，
        :param fieldMap: 包含各个特征的微博字段字典
        :return: None
        """
        # raise Exception('no implement exception', 'processOneWeibo method in Parser need implement !')
        return None

    def processOneWeiboModify(self, keyList, fieldMap):
        """
        根据需要处理每条微博，该方法由子类重写，此方法方便子类按字段的key 与 value 值进行操作
        然后使用 output(keyList, fieldMap)
        :param keyList: 微博字段key序列,方便按顺序输出
        :param fieldMap: 包含各个特征的微博字段字典
        :return:
        """
        # raise Exception('no implement exception', 'processOneWeibo method in Parser need implement !')
        return None

    def at2tab(self, keyList):
        """
        将以每条微博，以行的形式展开，每列以 tab 分割
        :param keyList:  每列所对应的key
        :return: 以行展开的微博
        """
        valList = []
        for key in keyList:
            val = self.__fieldMap[key] if key in self.__fieldMap else "null"
            if val:
                valList.append(val)
            else:
                valList.append("null")
        sys.stdout.write("\t".join(valList) + "\n")

    def flush(self):
        """
        处理缓存在 fieldMap 中的缓存的微博数据
        :return: None
        """
        if self.__fieldMap:
            self.processOneWeibo(self.__fieldMap)
            self.processOneWeiboModify(self.__fieldKeyList, self.__fieldMap)
            self.__fieldKeyList = []
            self.__fieldMap.clear()

    def outputWeibo(self):
        """
        将 fieldMap 中存储的微博原样输出
        :return: None
        """
        if self.__fieldMap:
            sys.stdout.write("@\n")
            for key in self.__fieldKeyList:
                sys.stdout.write("@{0}:{1}\n".format(key, self.__fieldMap[key]))

    def outputWeiboModify(self, keyList, fieldMap):
        """
        按给定的keyList 和 fieldMap 输入微博
        :param keyList: 微博字段key序列,方便按顺序输出
        :param fieldMap: 包含各个特征的微博字段字典
        :return:
        """
        if keyList:
            sys.stdout.write("@\n")
            for key in keyList:
                if key in fieldMap:
                    sys.stdout.write("@{0}:{1}\n".format(key, fieldMap[key]))
        else:
            if fieldMap:
                sys.stdout.write("@\n")
                for key, val in fieldMap.iteritems():
                    sys.stdout.write("@{0}:{1}\n".format(key, val))

    def __fieldSplit(self, line):
        """
        私有方法，仅提供给该类使用，
        将微博博分割后的内容，放入 fieldMap 与 keyList
        :param line: 微博内容
        :return: None
        """
        if line and line.startswith("@"):
            index = line.find(Parser.fieldSep)
            if index != -1:
                fieldName = line[1: index].strip()
                val = line[index + 1: len(line)]
                self.__fieldMap[fieldName] = val.strip()
                self.__fieldKeyList.append(fieldName)

    def __del__(self):
        """
        程序最后，处理fieldMap中缓存的数据
        :return:
        """
        self.flush()
