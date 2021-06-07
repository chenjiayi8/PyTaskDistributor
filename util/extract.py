# -*- coding: utf-8 -*-
"""
Created on Sat Nov 30 12:34:43 2019

@author: jiayichen
"""



def extractBetween(string, sub1, sub2):
    numSub1 = len(sub1)
    numSub2 = len(sub2)
    dict_sub1 = {i : string[i:i+numSub1] for i in range(len(string)-numSub1+1)}
    dict_sub2 = {i : string[i:i+numSub2] for i in range(len(string)-numSub2+1)}
    ind_sub1 = [i for i in range(len(dict_sub1)) if dict_sub1[i] == sub1]
    ind_sub2 = [i for i in range(len(dict_sub2)) if dict_sub2[i] == sub2]
    numPars = min(len(ind_sub1), len(ind_sub2))
    results = [string[ind_sub1[i]+numSub1:ind_sub2[i]] for i in range(numPars)]
    return results
    
def extractAfter(string, sub1):
    numSub1 = len(sub1)
    dict_sub1 = {i : string[i:i+numSub1] for i in range(len(string)-numSub1+1)}
    ind_sub1 = [i for i in range(len(dict_sub1)) if dict_sub1[i] == sub1]
    result = string[ind_sub1[-1]+numSub1:]
    return result

def extractBefore(string, sub1):    
    numSub1 = len(sub1)
    dict_sub1 = {i : string[i:i+numSub1] for i in range(len(string)-numSub1+1)}
    ind_sub1 = [i for i in range(len(dict_sub1)) if dict_sub1[i] == sub1]
    result = string[:ind_sub1[0]]
    return result
    
    
#    
#string = 'abc345defgabc123defg'
#sub1   = 'abc'
#sub2   = 'defg'    
#    
#result1 =  extractBetween(string, sub1, sub2)
#result2 =  extractAfter(string, sub1)
#result3 =  extractBefore(string, sub2)
#print(result1)
#print(result2)
#print(result3)


