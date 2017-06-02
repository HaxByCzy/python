# -*- coding: utf-8 -*-
"""
task	: K近邻算法尝试
input	: ***
output	: ***
@author	: baoquanZhang
update time	: 2017-05-04
"""

import sys
defaultencoding = 'utf-8'
if sys.getdefaultencoding() != defaultencoding:
    reload(sys)
    sys.setdefaultencoding(defaultencoding)
from sklearn.neighbors import NearestNeighbors
from sklearn import neighbors
import numpy as np
import matplotlib.pyplot as plt

# K近邻算法

def neighbors1():
    x = np.array([[-1, -1], [-2, -1], [-3, -2], [1, 1], [2, 1], [3, 2]])
    nbrs = NearestNeighbors(n_neighbors=2, algorithm='kd_tree').fit(x)
    distances, indices = nbrs.kneighbors(x)
    print distances
    print indices
    
    from sklearn.neighbors import KDTree
    kdt = KDTree(x, leaf_size=30, metric='euclidean')
    print kdt.query(x, k=2, return_distance=False)   

def neighbors2():
    x = np.array([[-1, -1], [-2, -1], [-3, -2], [1, 1], [2, 1], [3, 2]])
    y = np.array([1, 1, 1, 2, 2, 2])
    clf = neighbors.KNeighborsClassifier(n_neighbors = 2 , weights='distance')
    clf.fit(x,y)
    print(clf.predict([[-0.8, -1]]))

def neighbors3():
    from sklearn.datasets.samples_generator import make_classification
    X, Y = make_classification(n_samples=1000, n_features=2, n_redundant=0,
                             n_clusters_per_class=1, n_classes=3)
    #plt.scatter(X[:, 0], X[:, 1], marker='o', c=Y)
    #plt.show()
    print zip(X[:3],Y[:3])[:]
    #训练
    from sklearn import neighbors
    clf = neighbors.KNeighborsClassifier(n_neighbors = 15 , weights='distance')
    clf.fit(X, Y)
    #查看看预测的效果
    from matplotlib.colors import ListedColormap
    cmap_light = ListedColormap(['#FFAAAA', '#AAFFAA', '#AAAAFF'])
    cmap_bold = ListedColormap(['#FF0000', '#00FF00', '#0000FF'])
    #确认训练集的边界
    x_min, x_max = X[:, 0].min() - 1, X[:, 0].max() + 1
    y_min, y_max = X[:, 1].min() - 1, X[:, 1].max() + 1
    #生成随机数据来做测试集，然后作预测
    xx, yy = np.meshgrid(np.arange(x_min, x_max, 0.02),
                             np.arange(y_min, y_max, 0.02))
    Z = clf.predict(np.c_[xx.ravel(), yy.ravel()])
    
    # 画出测试集数据
    Z = Z.reshape(xx.shape)
    plt.figure()
    plt.pcolormesh(xx, yy, Z, cmap=cmap_light)
    
    # 也画出所有的训练集数据
    plt.scatter(X[:, 0], X[:, 1], c=Y, cmap=cmap_bold)
    plt.xlim(xx.min(), xx.max())
    plt.ylim(yy.min(), yy.max())
    plt.title("3-Class classification (k = 15, weights = 'distance')" )
    

if __name__ == "__main__":
	neighbors3()

