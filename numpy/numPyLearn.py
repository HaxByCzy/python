#coding:utf-8

import numpy as np
import random

a = np.array([1, 2, 3, 4, 5])

a = np.arange(20).reshape(4,5)

a = np.zeros((3, 4))

a = np.ones((4, 5))

a = np.arange(2, 20, 3)

#a = np.arange(10000).reshape(100, 100)

a = np.array([1,9,4,7,3,10,30,21,18])
a = np.sort(a)

a = np.floor(10*np.random.random((3,3)))
#np.savetxt("a.txt",a,delimiter=",")
a = np.loadtxt("a.txt", delimiter = ",")
print(a)

a = np.floor(10*np.random.random((3,3)))
b = np.floor(10*np.random.random((3,3)))
c = np.vstack((a, b))
d = np.hstack((a,b))


