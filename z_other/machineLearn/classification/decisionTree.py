# -*- coding: utf-8 -*-
"""
task	    : 决策树各类算法尝试
input	: iris
output	: 评估性能
@author	: baoquanZhang
update time	: 2017-03-28
"""

import sys
defaultencoding = 'utf-8'
if sys.getdefaultencoding() != defaultencoding:
    reload(sys)
    sys.setdefaultencoding(defaultencoding)
    
    
from sklearn.datasets import load_iris
from sklearn import tree

iris = load_iris()

# 尝试scikit提供的分类树模型
def decisionTree():
    clf = tree.DecisionTreeClassifier()
    clf = clf.fit(iris.data, iris.target)
    with open("D://iris.dot", 'w') as f:
        f = tree.export_graphviz(clf, out_file=f,
                         feature_names=iris.feature_names,  
                         class_names=iris.target_names,  
                         filled=True, rounded=True,  
                         special_characters=True) 
    from IPython.display import Image
    import pydotplus 
    dot_data = pydotplus.graph_from_dot_file("D://iris.dot")
    Image(dot_data.create_png())  
    dot_data.write_pdf("D://iris.pdf") 
    
        


if __name__ == "__main__":
	decisionTree()

