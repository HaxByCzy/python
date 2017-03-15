#coding:utf-8

import matplotlib.pyplot as plt
import numpy as np
import sys
defaultencoding = 'utf-8'
if sys.getdefaultencoding() != defaultencoding:
    reload(sys)
    sys.setdefaultencoding(defaultencoding)
from math import exp


def test1():
    plt.figure(figsize=(8, 4))  #创建一个绘图对象, 并设置对象的宽度和高度, 如果不创建直接调用plot, Matplotlib会直接创建一个绘图对象
    plt.plot([1, 2, 3, 4])  #此处设置y的坐标为[1, 2, 3, 4], 则x的坐标默认为[0, 1, 2, 3]在绘图对象中进行绘图, 可以设置label,
                            #  color和linewidth关键字参数
    plt.plot([6, 7, 8, 9])
    plt.ylabel('some numbers')  #给y轴添加标签, 给x轴加标签用xlable
    plt.xlabel("x axes")
    plt.title("hello");  #给2D图加标题
    plt.show()  #显示2D图

def test2():
    #绘制折线图
    x = [0, 1, 2, 4, 5, 6]
    y = [1, 2, 3, 2, 4, 1]
    plt.plot(x, y, '-*r')  # 虚线, 星点, 红色
    plt.xlabel("x-axis")
    plt.ylabel("y-axis")
    plt.show()

def test3():
    #多线图
    x = [0, 1, 2, 4, 5, 6]
    y = [1, 2, 3, 2, 4, 1]
    z = [1, 2, 3, 4, 5, 6]
    plt.plot(x, y, '--*r', x, z, '-.+g')
    plt.xlabel("x-axis")
    plt.ylabel("y-axis")
    plt.title("hello world")
    plt.show()

def test4():
    #柱状图
    x = [0, 1, 2, 4, 5, 6]
    y = [1, 2, 3, 2, 4, 1]
    z = [1, 2, 3, 4, 5, 6]
    plt.bar(x, y)
    plt.xlabel("x-axis")
    plt.ylabel("y-axis")
    plt.show()

def test5():
    #subplot()函数指明numrows行数, numcols列数, fignum图个数. 图的个数不能超过行数和列数之积
    x = [0, 1, 2, 4, 5, 6]
    y = [1, 2, 3, 2, 4, 1]
    z = [1, 2, 3, 4, 5, 6]
    #plt.figure(1)
    plt.subplot(211)
    plt.plot(x, y, '-+b')
    plt.subplot(212)
    plt.plot(x, z, '-.*r')
    plt.show()

def test6():
    #添加文本
    x = [0, 1, 2, 4, 5, 6]
    y = [1, 2, 3, 2, 4, 1]
    plt.plot(x, y, '-.*r')
    plt.text(1, 2, "I'm a text")  #前两个参数表示文本坐标, 第三个参数为要添加的文本
    plt.show()

def test7():
    #图例
    line_up, = plt.plot([1,2,3], label='Line 2')
    line_down, = plt.plot([3,2,1], label='Line 1')
    plt.legend(handles=[line_up, line_down])
    plt.show()

    line, = plt.plot([1, 2, 3])
    line.set_label("Label via method")
    plt.legend()
    plt.show()

def test8():
    #添加多图例
    line1, = plt.plot([1, 2, 3])
    line2, = plt.plot([3, 2, 1], '--b')
    plt.legend((line1, line2), ('line1', 'line2'))
    plt.show()

def test9():
    # x y轴最大最小值
    plt.plot([1,2,3,4],[1,4,9,16],'ro')
    plt.axis([0,6,0,20])
    #axis()函数接受形如[xmin,xmax,ymin,ymax]的参数，指定了X,Y轴坐标的范围
    plt.show()

def test10():
    t=np.arange(0.,5.,0.2)
    plt.plot(t,t,'r--',t,t**2,'bs',t,t**3,'g^')
    plt.show()

def test11():
    x = np.linspace(0, 10, 1000)
    y1 = 1 / (2 + x)
    y2 = np.sin(x)
    y2 = 1 / (1 + x)

    plt.plot(x, y1)
    plt.plot(x, y2)
    plt.show()


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
    x = np.linspace(0, 30, 100)

    y1 = expComputing(x, 5)
    y2 = expComputing(x, 7)
    y3 = expComputing(x, 9)

    plt.plot(x, y1)
    plt.plot(x, y2)
    plt.plot(x, y3)

    plt.show()

def expComputing(xlist, param):
    tmplist = []
    for x in xlist:
        y = (1 / (1 + exp(-x / param))) - 0.5
        yy = y / 0.5
        tmplist.append(yy)
    return np.array(tmplist)



if __name__ == "__main__":
    #test1()
    #test2()
    #test3()
    #test4()
    #test5()
    #test6()
    #test7()
    #test8()
    #test9()
    #test10()
    #test11()
    test12()
    #test13()





