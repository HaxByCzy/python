#coding:utf-8

import matplotlib.pyplot as plt
import numpy as np
import sys
defaultencoding = 'utf-8'
if sys.getdefaultencoding() != defaultencoding:
    reload(sys)
    sys.setdefaultencoding(defaultencoding)
from math import exp


def test12():
    x = np.linspace(0, 30, 1000)
    y1 = 1 / (2 + x)
    y2 = 1 / (4 + x)
    y3 = 1 / (8 + x)

    y11 = y1 / 0.5
    y22 = y2 / 0.25
    y33 = y3 / 0.125

    plt.xlabel('时间（间隔天数）')
    plt.ylabel('由时间得到的权重')
    #plt.plot(x , y1)
    plt.plot(x, y11)
    plt.plot(x, y22)
    plt.plot(x, y33)

    plt.show()


def test13():
    x = np.linspace(0, 110, 100)

    y1 = expComputing(x, 20)
    y2 = expComputing(x, 7)
    y3 = expComputing(x, 9)

    plt.xlabel("count")
    plt.ylabel("weight")
    plt.plot(x, y1)
    #plt.plot(x, y2)
    #plt.plot(x, y3)

    plt.show()

def expComputing(xlist, param):
    tmplist = []
    for x in xlist:
        y = (1 / (1 + exp(-x / param))) - 0.5
        yy = y / 0.5
        tmplist.append(yy)
  
    return np.array(tmplist)

def testNum():
    for x in range(1, 110):
        y = (1 / (1 + exp(-x / 20))) - 0.5
        yy = y / 0.5
        print str(x) + "\t" + str(yy)


if __name__ == "__main__":
    #testNum()
    test13()
    
    





















