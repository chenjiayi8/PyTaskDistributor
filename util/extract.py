# -*- coding: utf-8 -*-
"""
Created on Sat Nov 30 12:34:43 2019

@author: jiayichen
"""


def extract_between(string, sub1, sub2):
    num_sub1 = len(sub1)
    num_sub2 = len(sub2)
    dict_sub1 = {i: string[i:i + num_sub1] for i in range(len(string) - num_sub1 + 1)}
    dict_sub2 = {i: string[i:i + num_sub2] for i in range(len(string) - num_sub2 + 1)}
    ind_sub1 = [i for i in range(len(dict_sub1)) if dict_sub1[i] == sub1]
    ind_sub2 = [i for i in range(len(dict_sub2)) if dict_sub2[i] == sub2]
    num_pars = min(len(ind_sub1), len(ind_sub2))
    results = [string[ind_sub1[i] + num_sub1:ind_sub2[i]] for i in range(num_pars)]
    return results


def extract_after(string, sub1):
    num_sub1 = len(sub1)
    dict_sub1 = {i: string[i:i + num_sub1] for i in range(len(string) - num_sub1 + 1)}
    ind_sub1 = [i for i in range(len(dict_sub1)) if dict_sub1[i] == sub1]
    result = string[ind_sub1[-1] + num_sub1:]
    return result


def extract_before(string, sub1):
    num_sub1 = len(sub1)
    dict_sub1 = {i: string[i:i + num_sub1] for i in range(len(string) - num_sub1 + 1)}
    ind_sub1 = [i for i in range(len(dict_sub1)) if dict_sub1[i] == sub1]
    result = string[:ind_sub1[0]]
    return result

