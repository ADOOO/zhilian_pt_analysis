#!/usr/bin/env python
# -*- coding: utf-8 -*-

import requests
from Queue import Queue
import gevent
from bs4 import BeautifulSoup as bs
import re
import MySQLdb

HEADERS = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/45.0.2454.101 Safari/537.36",}

def spider(queue):
	while not queue.empty():
		url = queue.get_nowait()
		try:
			r = requests.get(url)
			soup = bs(r.content.replace('<b>','').replace('</b>',''), 'lxml')

			jobs = soup.find_all(name='td', attrs={'class':'zwmc'})
			#job.div.a.string
			companys = soup.find_all(name='td', attrs={'class':'gsmc'})
			#company.a.string
			wages = soup.find_all(name='td', attrs={'class':'zwyx'})
			#wages.string
			locations = soup.find_all(name='td', attrs={'class':'gzdd'})
			#location.string

			for job,company,wage,location in zip(jobs,companys,wages,locations):
				# print job.div.a.string,company.a.string,wage.string,location.string
				j = job.div.a.string
				c = company.a.string
				w = wage.string
				l = location.string
				job_detail_url = job.div.a['href']
				job_detail_req = requests.get(job_detail_url)
				contents = re.findall(r'SWSStringCutStart -->(.*?)<!-- SWSStringCutEnd', job_detail_req.content, re.S)
				content = re.sub(r'<[^>]+>', '', contents[0]).replace(' ','').replace('\r\n','').replace('&nbsp;','')
				print j,c,w,l
				print content.decode('utf-8')
				sqlin(j,c,w,l,content)

		except Exception,e:
			print e
			pass

def sqlin(j,c,w,l,content):
		conn = MySQLdb.connect(host = '10.211.55.9', user = 'ichunqiu', passwd = '123#@!', db = 'zhilian_pt', port = 3306, charset='utf8')
		cus = conn.cursor()
		sql = "insert into jobs (job,company,wages,location,content) VALUES ('%s','%s','%s','%s','%s')"%(j,c,w,l,content.decode('utf-8'))
		cus.execute(sql)
		cus = conn.commit()
		#cus.close()
		conn.close()


def create():
	queue = Queue()
	for i in range(1,15):
		queue.put('''http://sou.zhaopin.com/jobs/searchresult.ashx?jl=\
%E8%BE%93%E5%85%A5%E9%80%89%E6%8B%A9%E5%9F%8E%E5%B8%82&kw=%E6%B8%97%E9%80%8F%E6%B5%8B%E8%AF%95&sm=0&p='''+str(i))
	return queue

def main():
	queue = create()
	gevent_pool = []
	thread_count = 5
	for i in range(thread_count):
		gevent_pool.append(gevent.spawn(spider,queue))
	gevent.joinall(gevent_pool)


if __name__ == '__main__':
	main()