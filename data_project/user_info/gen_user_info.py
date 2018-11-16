# -*- coding: utf-8 -*-
"""
Created on Thu Oct 18 19:24:06 2018
@author zifeng
"""
import random
import ipaddress
import time
from fake_useragent import UserAgent

if __name__ == "__main__":
    fo = open("user_info.data",'w')
    
    def gen_age():
        return random.randint(16,80)
    
    def gen_sex():
        sex = ['男','女']
        return sex[random.randint(0,len(sex)-1)]
    
    def gen_industry():
        pages = ['互联网与售货目录零售','石油与天然气设备与服务','药品零售','半导体产品','大卖场与超市',
        '教育服务','化肥与农用药剂','航天航空与国防','互联网软件与服务','建筑产品','农产品','林业产品']
        return pages[random.randint(0,len(pages)-1)]
    
    def gen_job():
        pages = ['专业技术人员','行政人员','生产运输设备操作人员','商业服务业人员','农民','无业人员',
        '企业职工','事业单位职工']
        return pages[random.randint(0,len(pages)-1)]
    
    def gen_hobbies():
        pages = ['唱歌','听音乐','看电影','看韩剧','看综艺娱乐节目','看书','看小说','看杂志','逛街','购物','了解市场的行情','跳舞','演奏乐器','去健身房健身','减肥','塑形','瑜伽','足球','篮球','排球','跑步','羽毛球','乒乓球','保龄球','游泳','划船','水上娱乐','登山','郊游','钓鱼','养鱼','饲养宠物','玩网络游戏','单机游戏','上网聊天','论坛','贴吧','看新闻','摄影','摄像','旅游','自驾游','吃美食','做饭','做糕点','十字绣','织毛衣','做服装服饰','打扑克','麻将','睡觉','写字','练字','书法','下各种棋','美容','保养','化妆','打扮']
        return pages[random.randint(0,len(pages)-1)]

    for _ in range(0,100): 
        base_log = "{uid}\t{sex}\t{age}\t{industry}\t{job}\t{is_car}\t{is_house}\t{is_chilren}\t{hobbies}".format(
        uid = str(_),
        sex = gen_sex(),
        age = gen_age(),
        industry = gen_industry(),
        job = gen_job(),
        is_car = random.randint(0,1),
        is_house = random.randint(0,1),
        is_chilren = random.randint(0,1),
        hobbies = gen_hobbies()
        )
        fo.write(base_log+"\n")
        fo.close
