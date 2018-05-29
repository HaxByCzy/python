#-*- coding:utf-8 _*-  
""" 
--------------------------------------------------------------------
@function: 
@time: 2017-11-10 
author:baoquan3 
@version: 
@modify: 
--------------------------------------------------------------------
"""
import sys
import re

defaultencoding = 'utf-8'
if sys.getdefaultencoding() != defaultencoding:
    reload(sys)
    sys.setdefaultencoding(defaultencoding)

def regExpCheck(content):
    if "<sina:link " in content:
        start = content.index("<sina:link ")
        end = content.index("/>")
        tmp = content[0: start] + content[end + 2 : len(content)]
        content = tmp
    regExp = "[0-9]+.{1,3}[0-9]+"
    outList = re.findall(regExp, content)
    if outList:
        return outList
        #return ",".join(outList)
    else:
        return None

def specialNum():
    urlSet = set([
        "http://t.sina.com.cn/5708364496/FlirwDYu9",
        "http://t.sina.com.cn/5204121002/FmQiHcG1C"
    ])
    total = 0
    with open("d://data//dirllGraveSpecialNumTmp.dat", "w") as outFile:
        with open("d://data//content.dat") as inFile:
            for line in inFile:
                lineArr = line.strip().split("\t")
                if len(lineArr) == 2:
                    out = regExpCheck(lineArr[1])
                    if out:
                        total += 1
                        #outFile.write("\t".join([out, lineArr[0],  lineArr[1]]) + "\n")
                        if lineArr[0] in urlSet:
                            print out,"\t",line
                            for elem in out:
                                print elem.encode(encoding='utf-8'),"\t", elem, "\t"
                else:
                    print line
    print "total : " + str(total)



if __name__ == "__main__":
    specialNum()
    #regExpCheck("aa")