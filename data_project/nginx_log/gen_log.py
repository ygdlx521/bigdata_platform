#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Thu Oct 18 19:24:06 2018

@author: zifeng
"""
import random
import ipaddress
import time
from fake_useragent import UserAgent
from urllib import quote

if __name__ == "__main__":
    fo = open("20181019",'w')
    ua = UserAgent()   
    def gen_page():
        pages = ['index','movie','tv','show','comic','older','car','finance',
                 'amuse','children','IT']
        return pages[random.randint(0,len(pages)-1)]
    
    def gen_block():
        pages = ['up','down','left','right','middle','hidden']
        return pages[random.randint(0,len(pages)-1)]
    
    def gen_userid():
        return str(random.randint(0,999999))
    
    def gen_vid():
        return str(random.randint(0,628))
    
    def gen_referer_host():
        pages = ['baidu','iqiyi','wasu','mgtv','cctv','ifeng','pptv','1905',
                 'youku','qq','sohu']
        return "www."+pages[random.randint(0,len(pages)-1)]+".com"
    
    def gen_ip():
        return ipaddress.IPv4Address(random.randint(0, 2**32-1)).exploded

    def gen_time():
        return int(time.time())
    
    fi = open("../video_info/video_info.data",'r')
    title_list = []
    for line in fi:
        title = quote(line.split('\t')[1])
        title_list.append(title)
    fi.close()

    for _ in range(0,10): 
        base_log = "{ip} - - [{time}] \"GET /click.gif?page={page}&block={block}&vid={vid} HTTP/1.1\" 200 0 + 0 \"http://{refer_host}\" userid={userid} \"{ua}\"".format(
        time = gen_time(),
        ip = gen_ip(),
        page = gen_page(),
        block = gen_block(),
        vid = gen_vid(),
        refer_host = gen_referer_host(),
        userid = gen_userid(),
        ua = ua.random
        )
        fo.write(base_log+"\n")
        base_log = "{ip} - - [{time}] \"GET /view.gif?page={page}&block={block}&vid={vid} HTTP/1.1\" 200 0 + 0 \"http://{refer_host}\" userid={userid} \"{ua}\"".format(
        time = gen_time(),
        ip = gen_ip(),
        page = gen_page(),
        block = gen_block(),
        vid = gen_vid(),
        refer_host = gen_referer_host(),
        userid = gen_userid(),
        ua = ua.random
        )
        fo.write(base_log+"\n")
        base_log = "{ip} - - [{time}] \"GET /search.gif?page={page}&block=search_bar&wd={wd} HTTP/1.1\" 200 0 + 0 \"http://{refer_host}\" userid={userid} \"{ua}\"".format(
        time = gen_time(),
        ip = gen_ip(),
        page = gen_page(),
        block = gen_block(),
        wd = title_list[random.randint(0,len(title_list)-1)],
        refer_host = "ww.atguigu.com",
        userid = gen_userid(),
        ua = ua.random
        )
        fo.write(base_log+"\n")
        fo.close
    #print base_log
