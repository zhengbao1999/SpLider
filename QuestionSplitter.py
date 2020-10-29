#!/usr/bin/env python
# coding: utf-8

# In[16]:


import re
lines = []
Questions = []
Golds = []
QSplitted = []
GSplitted = []
col = []
lines = list(open("spider_multi_col_22000.txt", "r", encoding='utf-8').readlines())
# you may also want to remove whitespace characters like `\n` at the end of each line
for i in range(0, len(lines)):
    if(i%3 == 0):
        Questions.append(lines[i])
        QSplitted.append(re.split(', and |, | and |and |\n',lines[i]))
    if(i%3 == 1):
        Golds.append(lines[i])
        GSplitted.append(re.split(';| ,  | |\n',lines[i]))
        
for g in GSplitted:
    start = g.index("SELECT") + 1
    end = g.index("FROM")
    if(g[start] == "DISTINCT"):
        col.append(g[start+1:end])
    if(g[start] == "count(DISTINCT"):
        col.append(g[start:end])

    
weird = {"avg": "average", "min": "minimum", "max": "maximum", "sum": "sum", "count": "number of ", "count(DISTINCT": "number of different "}

testQ = []
testG = []
testQS = []
testGS = []
for i in range(0, 10):
    print(i)
    testQ.append(Questions[i])
    testG.append(Golds[i])
    testQS.append(QSplitted[i])
    testGS.append(GSplitted[i])
    print(Questions[i])
    print(QSplitted[i])
    print(Golds[i])
    print(GSplitted[i])


# In[ ]:





# In[ ]:





# In[ ]:




