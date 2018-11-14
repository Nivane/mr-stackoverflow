mr-stackoverflow: stackoverflow user and comment analysis with mapreduce
=====
## Introduction
mr-stackoverflow目的在于分析问答网站StackOverflow的数据，获取关联信息和行为，计算使用Hadoop框架的MapReduce，数据来源于数字图书馆组织[Internet Archive](https://archive.org/download/stackexchange)，本项目涵盖Users(2.6GB)和Comments(16.3GB)文件，文件为XML格式，详细XML Schema参考[StackExchange](https://meta.stackexchange.com/questions/2677/database-schema-documentation-for-the-public-data-dump-and-sede/2678#2678)
## Version
Java: 1.8.0_162
Hadoop: 2.7.4
## TOP10
644302 34397 SLaks http://SLaks.net
647540 100297 Martijn Pieters http://www.zopatista.com/
680565 1144035 Gordon Linoff http://www.data-miners.com
725875 115145 CommonsWare https://commonsware.com
738681 23354 Marc Gravell http://blog.marcgravell.com
755183 17034 Hans Passant null
777090 6309 VonC http://careers.stackoverflow.com/vonc
799191 29407 Darin Dimitrov http://stackoverflow.com/search?q=user%3a29407&amp;tab=newest
801719 157882 BalusC http://balusc.omnifaces.org
1029633 22656 Jon Skeet http://csharpindepth.com
是否添加了个人website信息 top10中有9个添加了 90%
是第多少个注册账号的人

##前10000个注册账号的人有多少人一年没有登录网站了

即筛选出账号ID<=10000，且最近登录时间超过一年的人有多少
