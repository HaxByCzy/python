#-*- coding:utf-8 _*-  
""" 
--------------------------------------------------------------------
@function:  动态规化经典例题
@time: 2017-10-17 
author:baoquan3 
@version: 
@modify: 
--------------------------------------------------------------------
"""
import sys
import numpy as np

defaultencoding = 'utf-8'
if sys.getdefaultencoding() != defaultencoding:
    reload(sys)
    sys.setdefaultencoding(defaultencoding)

def steps():
    """
    台阶问题, 类似于 斐波那契数列
    有n级台阶，一个人每次上一级或者两级，问有多少种走完n级台阶的方法。
    :return:
    """
    n = 5
    dp = []         # dp是全局数组，大小为n,全部初始化为0，是题目中的动态规划表
    for i in range(0, 5):
        dp.append(0)

    def fun(n):
        if n==1 or n==2:
            return n
        # 判断n-1的状态有没有被计算过
        if not dp[n-1]:
            dp[n-1] = fun(n-1)
        if not dp[n-2]:
            dp[n-2] = fun(n-2)
        return dp[n-1] + dp[n-2]

    print "5 级台阶可有走法是 ： " + str(fun(5))

def coinChange():
    """
    硬币找零：现存在一堆面值为 V1、V2、V3 … 个单位的硬币，问最少需要多少个硬币才能找出总值为 T 个单位的零钱？
    假设有几种硬币，如1、3、5，并且数量无限。请找出能够组成某个数目的找零所使用最少的硬币数。
    解题思路：
    通俗地讲，我们需要凑出 i 元，就在凑出 j 的结果上再加上某一个硬币就行了。那这里我们加上的是哪个硬币呢。嗯，其实很简单，把每个硬币试一下就行了：
    假设最后加上的是 1 元硬币，那 d(i) = d(j) + 1 = d(i - 1) + 1。
    假设最后加上的是 3 元硬币，那 d(i) = d(j) + 1 = d(i - 3) + 1。
    假设最后加上的是 5 元硬币，那 d(i) = d(j) + 1 = d(i - 5) + 1。
    我们分别计算出 d(i - 1) + 1，d(i - 3) + 1，d(i - 5) + 1 的值，取其中的最小值，即为最优解，也就是 d(i)
    从中可以得到状态转移方程为:d(n) = min{d[n-value[i]]+1}。
    :return:
    """
    # 硬币价值，和总钱数
    coinVal , money = [1, 3, 5], 63
    minCoinNum = 0
    dp = np.zeros(money + 1)
    # 用dp来存储组成一定总数所需的最小硬币数，其下标i为总数，dp[i]不所需硬币数
    for i in range(len(dp)):
        # 初始化一个很大的数值。当最后如果得出的结果是这个数时，说明凑不出来。
        dp[i] = 99999
    dp[0] = 0

    for cent in range(coinVal[0], money + 1):
        for j in range(0, len(coinVal)):
            if coinVal[j] <= cent:
                if dp[cent - coinVal[j]] + 1 < dp[cent]:
                    dp[cent]= dp[cent - coinVal[j]] + 1

    print "组成 " + str(money) + " 需要硬币数为 " + str(dp[money])



def maxSubSum():
    """
    最大子段和
    给定由n个整数（包含负整数）组成的序列a1,a2,...,an，求该序列子段和的最大值。当所有整数均为负值时定义其最大子段和为0。
    例如，当(a1,a2 , a3 , a4 , a5 ,a6)=(-2,11,-4,13,-5,-2)时，最大子段和为：11+（-4）+13 =20
    :return:
    """
    numSeq = [-2, 11, -4, 13, -5, -2]
    max, sum = numSeq[0], numSeq[0]     # 保存第一个值，保证当负数时，也可行
    for i in range(1, len(numSeq)):
        if sum < 0:
            sum = numSeq[i]
        else:
            sum += numSeq[i]

        if max < sum:
            max = sum
    print "最大子段和 ：" + str(max)


def notReduceSeq():
    """
    最长非降子序列
    在一个无序的序列a1,a2,.....,am里，找到一个最长的序列，满足ai<=aj...<=ak; 且i<j<k;
    比如arr=[2,1,5,3,6,4,8,9,7]，最长递增子序列为[1,3,4,8,9]返回其长度为5.
    :return:
    """
    numSeq = [4, 5, 7, 8, 3, 2, 6, 7, 33, 4]
    dp, max= [], 0
    for i in range(0, len(numSeq)):
        dp.append(0)
    dp[0] = 1
    for i in  range(1, len(numSeq)):
        dp[i] = 1
        for j in range(0, i):
            if numSeq[j] <= numSeq[i] and dp[j] + 1 > dp[i]:
                dp[i] = dp[j] + 1

    for elem in dp:
        if elem > max:
            max = elem
    print "max long not reduce seq : " + str(max)

def shareMaxLongSeq():
    """
    最长公共子序列
    两个字符串的最长公共子序列，例如：str1="1A2C3D4B56",str2="B1D23CA45B6A","123456"和"12C4B6"都是最长公共子序列，返回哪一个都行
    分析：本题是非常经典的动态规划问题，假设str1的长度为M，str2的长度为N，
        则生成M*N的二维数组dp，dp[i][j]的含义是str1[0..i]与str2[0..j]的最长公共子序列的长度。
    dp值的求法如下：
        dp[i][j]的值必然和dp[i-1][j],dp[i][j-1],dp[i-1][j-1]相关，结合下面的代码来看，我们实际上是从第1行和第1列开始计算的，而把第0行和第0列都初始化为0，这是为了后面的取最大值在代码实现上的方便，dp[i][j]取三者之间的最大值。
    :return:
    """
    #str1, str2 = "president", "providence"
    str1, str2 = "cnblogs", "belong"
    dp = np.zeros((len(str1) + 1, len(str2)+1))
    print len(str1) + 1, len(str2)+1, dp.shape
    def fun(str1, m, str2, n):
        """
        :param str1:
        :param m: 字符串一长度
        :param str2:
        :param n: 字符串二长度
        :return:
        """
        for i in range(1, m + 1):
            for j in range(1, n + 1):
                if str1[i-1] == str2[j-1]:
                    dp[i, j] = dp[i-1, j-1] + 1
                else:
                    dp[i, j] = max(dp[i-1, j], dp[i, j-1])
        return dp
    maxLong = 0
    for elem in dp:
        tmpMax = np.max(elem)
        if tmpMax > maxLong:
            maxLong = tmpMax
    print "最长公共子序列为 ： " + str(maxLong)
    print dp

def bag():
    """
    背包问题
    有n 个物品，它们有各自的重量和价值，现有给定容量的背包，如何让背包里装入的物品具有最大的价值总和？
    :return:
    """
    # 物品的数量与背包的容量
    num, capacity = 4, 10
    value = [42, 12, 40, 25]
    weight = [7, 3, 4, 5]

    num, capacity = 3, 10
    value = [20, 10, 12]
    weight = [5, 4, 3]
    dp = np.zeros((num + 1, capacity + 1))
    for i in range(1, num + 1):             # 遍历物品
        for j in range(1, capacity + 1):    # 遍历重量
            if j - weight[i-1] > 0:           # 判断枚举的重量和当前选择的物品重量的关系，如果枚举的和总量大于等于选择物品，则需要判断是否选择当前物品
                dp[i,j] = max(dp[i-1, j], dp[i-1, j - weight[i - 1]] + value[i - 1])
            else:       # 如果枚举的重量还没有当前选择物品的重量大，那就只能是不取当前物品
                dp[i,j] = dp[i-1, j]
    print dp

if __name__ == "__main__":
    # steps()
    coinChange()
    # maxSubSum()
    # notReduceSeq()
    # shareMaxLongSeq()
    #bag()

