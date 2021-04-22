#!/usr/bin/env python
# -*- coding:utf-8 -*-

""" 
:Description: 
:Owner: tao_chen
:Create time: 2021/04/22
leet code 455. 分发饼干
贪心 + 双指针
"""


class Solution(object):
    def findContentChildren(self, g, s):
        """
        :type g: List[int]
        :type s: List[int]
        :rtype: int
        """
        g_sorted_ascending = sorted(g)
        s_sorted_ascending = sorted(s)
        i = 0
        j = 0
        while i < len(g_sorted_ascending) and j < len(s_sorted_ascending):
            if g_sorted_ascending[i] <= s_sorted_ascending[j]:
                i = i + 1
                j = j + 1
            else:
                j = j + 1
        return i


if __name__ == '__main__':
    g_example = [10,9,8,7]
    s_example = [5,6,7,8]

    res = Solution().findContentChildren(g_example, s_example)
    print res
