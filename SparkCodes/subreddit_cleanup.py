# -*- coding: utf-8 -*-
"""
Created on Fri Jan 29 21:43:33 2016

@author: jprawiharjo
"""

f = open('redditwhitelistraw.txt')
fw = open('redditwhitelist.txt','w')

whitelist=[]
for k in f:
    s = k.split(" ")
    if len(s) == 2:
        num =  int(s[1].replace(',', ''))
        ss = "{0} {1}".format(s[0],num)
        if 'politics' in s[0]:
            print ss
        whitelist.append(s[0])
        fw.write(s[0]+"\n")
        

#fw.writelines(whitelist)