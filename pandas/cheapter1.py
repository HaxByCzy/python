#coding:utf-8

from pandas import Series,DataFrame
import pandas as pd
import numpy as np

# series基本使用
def seriesTest1():
    s1 = pd.Series(["zhang", "bao", "quan"])
    for val in s1.values:
        print(val)

# series基本使用
def seriesTest2():
    mydict = {"a": 1, "b":2, "c": 3, "d" : 4}
    se = pd.Series(mydict,index = ["a", "b", "c", "d", "e"])
    print(se)
    print(pd.isnull(se))

# dataFrame基本使用
def dataFrameTest1():
    data = {"name":["yahoo","google","facebook"], "marks":[200,400,800], "price":[9, 3, 7]}
    df1 = pd.DataFrame(data)
    df2 = pd.DataFrame(data,columns=["name", "price", "marks", "debt"])
    print(df1)
    print("-----------")
    print(df2)
    print("-----------")
    for val in df1.columns:
        print(val)
    print("-----------")
    print(df1["name"])

# dataFrame基本使用
def dataFrameTest2():
     data = {"name":["yahoo","google","facebook"], "marks":[200,400,800], "price":[9, 3, 7]}
     df1 = pd.DataFrame(data)
     sdebt = pd.Series([2.2, 3.3], index=[0,2])
     df1["debt"] = sdebt
     print(df1)
     print("----------")
     df1["debt"][1] = 2.7
     print(df1)

# pandas文件操作
def dataFrameFile1():
    data = pd.read_csv("marks.csv", sep = ",")
    print(data.index)
    print("----------")
    print(data.columns)
    print("--------------")
    print(data)

# pandas文件数据基本操作
def dataFrameFile2():
    data = pd.read_csv("marks.csv", sep = ",")
    print(data.sort(columns="python"))
    print("----------")
    print(data)


# pandas文件数据基本操作
def dataFrameFile3():
    data = pd.read_csv("marks.csv", sep = ",")
    slice1 = data[2:]
    print(slice1)
    print("----------")
    print(data)

# pandas文件数据基本操作
def dataFrameFile4():
    data = pd.read_csv("marks.csv", sep = ",")
    slice1 = data.ix[2:,2:]
    slice2 = data.iloc[2:,2:]
    print(slice1)
    print("-----------")
    print(slice2)
    print("----------")
    print(data)


# pandas文件数据基本操作
def dataFrameFile5():
    data = pd.read_csv("marks.csv",header = None, sep = ",")
    print(data)
    data.to_csv('out.csv', header=None, index = None)

# pandas文件数据基本操作
def dataFrameFile6():
    data = pd.read_csv("marks.csv", sep = ",")
    print(data.mean())
    print("----------")
    print(data.sum())
    print("----------")
    print(data)

def readExcel():
    data = pd.read_csv("marks.csv", sep = ",")
    data1 = data.drop(3, axis = 0)
    print data1
    data2 = data.drop("name",axis = 1)
    print data2



if __name__ == "__main__":
    dataFrameFile5()
