--################### 网站自身数据 ###################
--1.各页面展现 dwm
select 
url_params['page'] as page,
count(*) as pv,
count(distinct userid) as uv
from 
dwd_user_view_event_d
where concat(year,month,day) = '20181019'
group by url_params['page']
;

--2.各页面点击 dwm
select 
url_params['page'] as page,
count(*) as pv,
count(distinct userid) as uv
from 
dwd_user_click_event_d
where concat(year,month,day) = '20181019'
group by url_params['page']
;

--3.各页面各区块点击 dwm
select 
url_params['page'] as page,
url_params['block'] as block,
count(*) as pv,
count(distinct userid) as uv
from 
dwd_user_click_event_d
where concat(year,month,day) = '20181019'
group by url_params['page'],url_params['block']
;

--4.流量来源 dwm
select 
url_params['frm'],
count(*) as pv,
count(distinct userid) as uv
from 
dwd_user_view_event_d
where concat(year,month,day) = '20181019'
group by url_params['frm']
;

--5.流量来源到各页面来源 dwm
select 
url_params['frm'],
url_params['page'],
count(*) as pv,
count(distinct userid) as uv
from 
dwd_user_view_event_d
where concat(year,month,day) = '20181019'
group by url_params['frm'],url_params['page']
;

--6.总点击跳出链接 dwm
select 
url_params['url']
count(*) as pv,
count(distinct userid) as uv
from 
dwd_user_view_event_d
where concat(year,month,day) = '20181019'
group by url_params['url']
;

--7.各页面点击跳出链接 dwm
select 
url_params['page'],
url_params['url']
count(*) as pv,
count(distinct userid) as uv
from 
dwd_user_view_event_d
where concat(year,month,day) = '20181019'
group by url_params['page'],url_params['url']
;

--8.网站总日活 app
select
count(distinct tb1.userid) as uv
from
(
	select 
	userid
	from 
	dwd_user_view_event_d
	where concat(year,month,day) = '20181019'
union all
select 
	userid
	from 
	dwd_user_click_event_d
	where concat(year,month,day) = '20181019'
union all
	select 
	userid
	from 
	dwd_user_search_event_d
	where concat(year,month,day) = '20181019'
)tb1
;

--9.检索词频 dwm
select 
url_params['wd']
count(*) as pv
from 
dwd_user_search_event_d
where concat(year,month,day) = '20181019'
group by url_params['wd']
order by pv desc
;

--10.视频观看次数、人数 dwm
select 
url_params['vid'],
count(*) as pv,
count(distinct userid) as uv
from 
dwd_user_click_event_d
where concat(year,month,day) = '20181019'
group by url_params['vid']
order by pv desc
;


--11.用户主题访问的页面数、点击次数、检索次数、视频观看数 dwm
select
*
from
(
	select 
	userid,
	count(*) as pv
	from 
	dwd_user_view_event_d
	where concat(year,month,day) = '20181019'
	group by userid
)tb1
full outer join
(
	select 
	userid,
	count(*) as pv
	from 
	dwd_user_click_event_d
	where concat(year,month,day) = '20181019'
	group by userid
)tb2 
on tb1.userid = tb2.userid
full outer join
(
	select 
	userid,
	count(*) as pv
	from 
	dwd_user_click_event_d
	where concat(year,month,day) = '20181019'
	group by userid
)tb3 
on tb1.userid = tb3.userid
full outer join
(
	select 
	userid,
	count(*) as pv
	from 
	dwd_user_click_event_d
	where concat(year,month,day) = '20181019'
	and url_params['vid'] is not null
	group by userid
)tb4
on tb1.userid = tb4.userid
;

--12.浏览器主题的访问页面数、人数、点击次数、人数 dwm
select
*
from
(
	select 
	browser,
	count(*) as pv,
	count(distinct userid) as uv
	from 
	dwd_user_view_event_d
	where concat(year,month,day) = '20181019'
	group by browser
)tb1
full outer join
(
	select 
	browser,
	count(*) as pv,
	count(distinct userid) as uv
	from 
	dwd_user_click_event_d
	where concat(year,month,day) = '20181019'
	group by browser
)tb2 
on tb1.userid = tb2.userid

--13.流量来源主题用户访问页面数、点击次数、检索次数、视频观看数 dwm
select
*
from
(
	select 
	url_params['frm'],
	count(*) as pv
	from 
	dwd_user_view_event_d
	where concat(year,month,day) = '20181019'
	group by url_params['frm']
)tb1
full outer join
(
	select 
	url_params['frm'],
	count(*) as pv
	from 
	dwd_user_click_event_d
	where concat(year,month,day) = '20181019'
	group by url_params['frm']
)tb2 
on tb1.url_params['frm'] = tb2.url_params['frm']
full outer join
(
	select 
	url_params['frm'],
	count(*) as pv
	from 
	dwd_user_click_event_d
	where concat(year,month,day) = '20181019'
	group by url_params['frm']
)tb3 
on tb1.url_params['frm'] = tb3.url_params['frm']
full outer join
(
	select 
	url_params['frm'],
	count(*) as pv
	from 
	dwd_user_click_event_d
	where concat(year,month,day) = '20181019'
	and url_params['vid'] is not null
	group by url_params['frm']
)tb4
on tb1.url_params['frm'] = tb4.url_params['frm']
;

--14.各国家、省份、地区用户量 dm
select
country,
province,
city,
count(distinct tb1.userid) as uv
(
	select 
	userid
	from 
	dwd_user_view_event_d
	where concat(year,month,day) = '20181019'
union all
	select 
	userid
	from 
	dwd_user_click_event_d
	where concat(year,month,day) = '20181019'
union all
	select 
	userid
	from 
	dwd_user_search_event_d
	where concat(year,month,day) = '20181019'
)tb1
group by country,province,city
;

--15.各国家、省份、地区展现量 dm
select
country,
province,
city,
count(distinct tb1.userid) as uv
(
	select 
	userid
	from 
	dwd_user_view_event_d
	where concat(year,month,day) = '20181019'
)tb1
group by country,province,city
;

--16.各国家、省份、地区点击量 dm
select
country,
province,
city,
count(distinct tb1.userid) as uv
(
	select 
	userid
	from 
	dwd_user_click_event_d
	where concat(year,month,day) = '20181019'
)tb1
group by country,province,city
;

--17.各国家、省份、地区检索量 dm
select
country,
province,
city,
count(distinct tb1.userid) as uv
(
	select 
	userid
	from 
	dwd_user_others_event_d
	where concat(year,month,day) = '20181019'
)tb1
group by country,province,city
;

--################### 行为、用户信息关联分析 ###################
--18.观看某一个视频的用户性别信息分析
select
tb2.sex,
count(*) as uv
from
(
	select 
	distinct userid
	from 
	event.dwd_user_view_event_d
	where concat(year,month,day) = '20181019'
	and url_params['vid'] = '0'
)tb1
left outer join
(
	select 
	uid,
	sex,
	age,
	industry,
	job,
	hobbies
	from 
	dimension.dim_user_info
	where concat(year,month,day) = '20181019'
)tb2
on tb1.userid = tb2.uid
group by tb2.sex
;

--19.观看某一个视频的用户爱好信息分析
select
tb2.hobbies,
count(*) as uv
from
(
	select 
	distinct userid
	from 
	event.dwd_user_view_event_d
	where concat(year,month,day) = '20181019'
	and url_params['vid'] = '0'
)tb1
left outer join
(
	select 
	uid,
	sex,
	age,
	industry,
	job,
	hobbies
	from 
	dimension.dim_user_info
	where concat(year,month,day) = '20181019'
)tb2
on tb1.userid = tb2.uid
group by tb2.hobbies
;

--20.观看某一个视频的用户的地域分布
select 
country,
province,
city,
count(distinct userid) as uv
from 
event.dwd_user_view_event_d
where concat(year,month,day) = '20181019'
and url_params['vid'] = '0'
group by country,province,city

--21.观看某一个视频的用户还检索了哪些词
select
tb2.wd,
count(*) as pv
from
(
	select 
	distinct userid
	from 
	event.dwd_user_view_event_d
	where concat(year,month,day) = '20181019'
	and url_params['vid'] = '0'
)tb1
left outer join
(
	select 
	userid,
	url_params['wd'] as wd 
	from 
	event.dwd_user_others_event_d
	where concat(year,month,day) = '20181019'
)tb2
on tb1.userid = tb2.userid
group by tb2.wd
;
、
--22.观看某一个视频的用户还看了哪些视频
select
tb2.vid,
count(*) as pv
from
(
	select 
	distinct userid
	from 
	event.dwd_user_view_event_d
	where concat(year,month,day) = '20181019'
	and url_params['vid'] = '0'
)tb1
left outer join
(
	select 
	userid,
	url_params['vid'] as vid 
	from 
	event.dwd_user_view_event_d
	where concat(year,month,day) = '20181019'
)tb2
on tb1.userid = tb2.userid
group by tb2.vid
;

--################### 行为、视频关联分析 ###################
--23.哪个导演的视频最受人欢迎
select
tb2.director,
count(*) as uv
from
(
	select 
	url_params['vid'] as vid
	from 
	event.dwd_user_view_event_d
	where concat(year,month,day) = '20181019'
)tb1
left outer join
(
	select 
	vid,
	director
	from 
	dimension.dim_video_info
	where concat(year,month,day) = '20181019'
)tb2
on tb1.vid = tb2.vid
group by tb2.director
;

--24.哪个主演的视频人们最爱看
select
tb2.actor,
count(*) as uv
from
(
	select 
	url_params['vid'] as vid
	from 
	event.dwd_user_view_event_d
	where concat(year,month,day) = '20181019'
)tb1
left outer join
(
	select 
	vid,
	actor
	from 
	dimension.dim_video_info
	where concat(year,month,day) = '20181019'
)tb2
on tb1.vid = tb2.vid
group by tb2.actor
;

--################### 行为、视频、用户信息关联分析 ###################

