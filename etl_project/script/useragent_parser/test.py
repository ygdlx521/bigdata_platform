from user_agents import parse
ua_string = 'Mozilla/5.0 (iPhone; CPU iPhone OS 5_1 like Mac OS X) AppleWebKit/534.46 (KHTML, like Gecko) Version/5.1 Mobile/9B179 Safari/7534.48.3'
print parse(ua_string)
f=open('broswer_ua','r')
w=open('new_broswer_ua','w')
li=f.readline()
while li:
    tem=li.strip().split('\t')[1]
    tt=parse(tem).browser.family
    w.write(tt+'\t'+li)
    li=f.readline()
    
