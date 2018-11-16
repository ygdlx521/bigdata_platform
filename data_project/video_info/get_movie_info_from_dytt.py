# -*- coding=utf-8 -*-
import urllib2
import os
import re
import string


vid  = 0
# 获取电影列表
def queryMovieList(url):
    conent = urllib2.urlopen(url)
    conent = conent.read()
    conent = conent.decode('gb2312','ignore').encode('utf-8','ignore') 
    pattern = re.compile ('<div class="title_all"><h1><font color=#008800>.*?</a>></font></h1></div>'+
         '(.*?)<td height="25" align="center" bgcolor="#F4FAE2"> ',re.S)
    items = re.findall(pattern,conent) 
    str = ''.join(items)
    pattern = re.compile ('<a href="(.*?)" class="ulink">(.*?)</a>.*?<td colspan.*?>(.*?)</td>',re.S)
    news = re.findall(pattern, str)
    movieUrls = []
    for j in news:
       movieUrls.append('http://www.dytt8.net'+j[0])
    return movieUrls

def queryMovieInfo(movieUrls,fo):
    global vid
    for index, url in enumerate(movieUrls):
        #print('电影URL: ' + item)
        conent = urllib2.urlopen(url)
        conent = conent.read()
        conent = conent.decode('gb2312','ignore').encode('utf-8','ignore') 
        movieName = re.findall(r'<div class="title_all"><h1><font color=#07519a>(.*?)</font></h1></div>', conent, re.S)
        if (len(movieName) > 0):
            movieName = movieName[0] + ""
            # 截取名称
            movieName = movieName[movieName.find("《") + 3:movieName.find("》")]
        else:
            movieName = ""
        #print("电影名称: " + movieName.strip())
        movieContent = re.findall(r'<div class="co_content8">(.*?)</tbody>',conent , re.S)
        pattern = re.compile('<ul>(.*?)<tr>', re.S)
        try:
            movieDate = re.findall(pattern,movieContent[0])
        except Exception,err:
            print movieName
            print Exception,err
            movieDate = ""
        if (len(movieDate) > 0):
            movieDate = movieDate[0].strip().split("：")[1] + ''
        else:
            movieDate = ""
        #print("电影发布时间: " + movieDate[-10:])
        pattern = re.compile('<br /><br />(.*?)<br /><br /><img')
        try:
            movieInfo = re.findall(pattern, movieContent[0])
        except Exception,err:
            print movieName
            print Exception,err
            movieInfo = ""
        if (len(movieInfo) > 0):
            movieInfo = movieInfo[0]+''
            # 删除<br />标签
            movieInfo = movieInfo.replace("<br />","")
            # 根据 ◎ 符号拆分
            movieInfo = movieInfo.split('◎')
        else:
            movieInfo = ""
        #print("电影基础信息: ")
        area = ""
        year = ""
        category = ""
        language = ""
        score = ""
        length = ""
        director = ""
        actor = ""
        for item in movieInfo:
            #print item
            try:
                if "产　　地" in item:
                    area = item.replace("　　","").split("　")[1].strip()
                elif "年　　代" in item:
                    year = item.replace("　　","").split("　")[1].strip()
                elif "类　　别" in item:
                    category = item.replace("　　","").split("　")[1].strip()
                elif "语　　言" in item:
                    language = item.replace("　　","").split("　")[1].strip()
                elif "豆瓣评分" in item:
                    score = item.replace("　　","").split("　")[1].strip()
                elif "片　　长" in item:
                    length = item.replace("　　","").split("　")[1].strip()
                elif "导　　演" in item:
                    director = item.replace("　　","").split("　")[1].strip()
                elif "主　　演" in item:
                    actor = item.replace("　　","").split("　")[1].strip()
            except Exception,err:
                print Exception,err
        # 电影海报
        pattern = re.compile('<img.*? src="(.*?)".*? />', re.S)       
        try:
            movieImg = re.findall(pattern,movieContent[0])
        except Exception,err:
            print movieName
            print Exception,err
            movieImg = ""
        if (len(movieImg) > 0):
            movieImg = movieImg[0]
        else:
            movieImg = ""
        #print("电影海报: " + movieImg)
        pattern = re.compile('<td style="WORD-WRAP: break-word" bgcolor="#fdfddf"><a href="(.*?)">.*?</a></td>', re.S)
        try:
            movieDownUrl = re.findall(pattern,movieContent[0])
        except Exception,err:
            print movieName
            print Exception,err
            movieDownUrl = ""
        if (len(movieDownUrl) > 0):
            movieDownUrl = movieDownUrl[0]
        else:
            movieDownUrl = ""
        #print("电影下载地址：" + movieDownUrl + "")
        fo.write(str(vid)+"\t"+movieName+"\t"+url+"\t"+movieDate+"\t"
                +year+"\t"+area+"\t"+category+"\t"+language+"\t"+score+"\t"
                +length+"\t"+director+"\t"+actor+"\t"+movieDownUrl+"\n")
        #print("------------------------------------------------\n\n\n")
        vid += 1

if __name__=='__main__':
    print("开始抓取电影数据")
    fo = open("video_info.data",'w')
    for i in range(2,101): 
        url = "http://www.dytt8.net/html/gndy/dyzz/list_23_"+str(i)+".html"
        movieUrls = queryMovieList(url)
        print url
        print(len(movieUrls))
        queryMovieInfo(movieUrls,fo)
    fo.close()
    print("结束抓取电影数据")
