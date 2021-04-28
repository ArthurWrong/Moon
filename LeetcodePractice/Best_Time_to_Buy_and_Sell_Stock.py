#!/usr/bin/env python
# -*- coding:utf-8 -*-

""" 
:Description: 
:Owner: tao_chen
:Create time: 2021/04/22
"""


class Solution(object):
    def maxProfit(self, prices):
        """
        :type prices: List[int]
        :rtype: int
        """
        index_prices = []
        for index, price in enumerate(prices):
            index_prices.append([index, price])

        prices_descending = sorted(index_prices, key=lambda x: -x[1])
        i = 0
        j = len(prices_descending) - 1
        max_diff = 0
        while i < j:
            if index_prices[i][0] < index_prices[j][0]:
                max_diff = index_prices[i][1] < index_prices[j][1]
            else:
        return max_diff


if __name__ == '__main__':
    res = Solution().maxProfit([7, 1, 5, 3, 6, 4])
    print res
