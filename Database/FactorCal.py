# -*- coding: utf-8 -*-
# Leo70kg
"""因子计算公式，内部函数，在FactorDataStore中调用"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import talib as ta


class TechnicalFactor(object):
    """kl_df为从数据库中获取的单合约行情数据"""
    @classmethod
    def moving_average(cls, kl_df, n):
        """计算移动平均"""
        return kl_df['close'].rolling(window=n[0]).mean()

    @classmethod
    def atr(cls, kl_df, n):
        """计算ATR指标"""
        return ta.ATR(kl_df.high.values.astype(float), kl_df.low.values.astype(float), kl_df.close.values.astype(float),
                      timeperiod=n[0])

    @classmethod
    def rsi(cls, kl_df, n):

        def algorithm_rsi(pse):
            return float((pse > 0).sum()) / float(len(pse))

        return kl_df['log_return'].rolling(window=n[0]).apply(func=algorithm_rsi)

    @classmethod
    def dsi(cls, kl_df, n):

        def algorithm_dsi(pse):
            return sum(pse) / sum(abs(pse))

        return kl_df['log_return'].rolling(window=n[0]).apply(func=algorithm_dsi)
