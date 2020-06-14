# coding=gbk

import pandas as pd
import numpy as np
import datetime as dt
# from tqdm import tqdm
import openpyxl
import xlrd
from WindPy import w
w.start()

# --工具函数--
def change_unit(df, field, denominator):
    # --更换多列单位--
    for each_field in field:
        df[each_field] = df[each_field] / denominator
    return df

def horizontal_merge(past, latest_tmp, field, sort_by,sort_ascending=False, first_download=False):
    # --横向合并--
    if first_download == True: 
        latest = latest_tmp
    else:
        latest = past.merge(latest_tmp, how='left', on=field)
    latest.sort_values(by=sort_by, ascending=sort_ascending, inplace=True)
    return latest

def vertical_merge(past, latest_tmp, sort_by, sort_ascending=True):
    # --纵向合并--
    latest = pd.concat([past, latest_tmp], axis=0, ignore_index=True)
    latest.sort_values(by=sort_by, ascending=sort_ascending, inplace=True)
    return latest

def get_monday(date_tmp):
    # --获取date_tmp所在周的周一的日期--
    date = dt.datetime.strptime(date_tmp, '%Y-%m-%d')
    monday_tmp = date
    one_day = dt.timedelta(days=1)
    while monday_tmp.weekday() != 0:
        monday_tmp -= one_day
    monday = monday_tmp.strftime('%Y-%m-%d')
    return monday

# --获取资金数据--
def get_volume_data_week(end_date):
    start_date = get_monday(end_date)
    sh_volume = w.wset("szhktransactionstatistics",
                       "startdate={};enddate={};cycle=week;currency=hkd;field=date,sz_total_amount,sz_buy_amount,sz_sell_amount".format(
                           start_date, end_date)).Data
    sz_volume = w.wset("shhktransactionstatistics",
                       "startdate={};enddate={};cycle=week;currency=cny;field=date,sh_total_amount,sh_buy_amount,sh_sell_amount".format(
                           start_date, end_date)).Data

    etf_info = pd.read_excel("ETF信息.xlsx", "全部ETF")
    etf_code = [','.join(etf_info.证券代码)]
    etf_volume = pd.DataFrame(w.wss(etf_code, "mrg_long_amt_int,margin_shortamountint,amt_per",
                                    "unit=1;startDate={};endDate={}".format(start_date, end_date)).Data).T
    etf_volume.columns = ['mrg_long_amt', 'margin_saletradingamount', 'amt']
    mkt_volume = w.wss("000001.SH,399001.SZ,000300.SH,000905.SH,399006.SZ", "amt_per",
                       "unit=1;startDate={};endDate={}".format(start_date, end_date)).Data
    margin_volume = w.wset("marginshortsizeanalysis(value)",
                           "exchange=shsz;startdate={};enddate={};frequency=week;sort=asc;field=end_date,total_trade_amount,margin_purchase_amount,margin_sell_amount".format(
                               start_date, end_date)).Data
    volume_dict = {'日期': end_date,
                   '总成交额': (mkt_volume[0][0] + mkt_volume[0][1]) / 100000000,
                   '沪市成交额': mkt_volume[0][0] / 100000000,
                   '深市成交额': mkt_volume[0][1] / 100000000,
                   '沪深300成交额': mkt_volume[0][2] / 100000000,
                   '中证500成交额': mkt_volume[0][3] / 100000000,
                   '创业板成交额': mkt_volume[0][4] / 100000000,
                   '其他成交额': ((mkt_volume[0][0] + mkt_volume[0][1]) - mkt_volume[0][2] - mkt_volume[0][3] -
                             mkt_volume[0][4]) / 100000000,
                   '沪股通成交额': sh_volume[1][0],
                   '深股通成交额': sz_volume[1][0],
                   '两融成交额': margin_volume[1][0] / 100000000,
                   'ETF成交额（除两融）': (etf_volume.amt.sum() - etf_volume.mrg_long_amt.sum() - etf_volume.margin_saletradingamount.sum()) / 100000000,
                   '融资买入额': margin_volume[2][0] / 100000000,
                   '融资卖出额': margin_volume[3][0] / 100000000,
                   '沪股通买入额': sh_volume[2][0],
                   '沪股通卖出额': sh_volume[3][0],
                   '深股通买入额': sz_volume[2][0],
                   '深股通卖出额': sz_volume[3][0],
                   'ETF成交额': etf_volume.amt.sum() / 100000000,
                   'ETF融资买入额': etf_volume.mrg_long_amt.sum() / 100000000,
                   'ETF融资卖出额': etf_volume.margin_saletradingamount.sum() / 100000000,
                   }
    volume = pd.DataFrame(volume_dict, index=[0])
    # --新旧数据合并--
    volume_past = pd.read_excel('成交量周度.xlsx')
    volume_latest = pd.concat([volume_past, volume], axis=0, ignore_index=True)
    # --结果保存--
    xlwriter = pd.ExcelWriter('成交量周度.xlsx')
    volume_latest.to_excel(xlwriter, '成交量周度', index=False)
    xlwriter.save()
    print("----{} 成交额(周度) 下载完成----".format(end_date))


if __name__ == '__main__':

    date='2020-06-07'
    get_volume_data_week(date)
