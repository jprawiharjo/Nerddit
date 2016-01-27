# -*- coding: utf-8 -*-
"""
Created on Tue Jan 26 21:28:55 2016

@author: jprawiharjo

"""

fopen = open("ForeignLanguage.txt")
fw = open("foreignsubredditlist.txt",'w')

srlist = []
for k in fopen:
    fsplit = k.rstrip().split(" ")
    for l in fsplit:
        ffsplit = l.split("/")[2]
        srlist.append(ffsplit)

fw.writelines(srlist)