# -*- coding：utf-8 -*-

import re
import pymysql

from time import sleep
from threading import Thread
from bs4 import BeautifulSoup
from urllib import (request, error, parse)

headers = {'Host': 'sou.zhaopin.com',
           'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
           'Referer': 'http://sou.zhaopin.com/jobs/searchresult.ashx',
           'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 '
                         '(KHTML, like Gecko) Chrome/62.0.3202.89 Safari/537.36'}

job_loc_list = ['北京', '上海', '广州', '深圳', '天津', '武汉', '西安', '成都', '大连', '长春', '沈阳', '南京'
                '济南', '青岛', '杭州', '苏州', '无锡', '宁波', '重庆', '郑州', '长沙', '福州', '厦门', '哈尔滨'
                '石家庄', '合肥', '惠州']


class ZLSpider(object):
    """
    智联工作信息爬取
    """
    def __init__(self):
        self.url_base = 'http://sou.zhaopin.com/jobs/searchresult.ashx?'
        self.conn = pymysql.connect(host='localhost', user='root', password='yuyaodong2587', charset='utf8')
        self.cur = self.conn.cursor()
        self.cur.execute('use ZL_job_info')
        self.html_pool = []
        self.parse_pool = []

    # 接收工作名称关键字
    @property
    def job_name_cmd_get(self):
        return self._job_name

    @job_name_cmd_get.setter
    def job_name_cmd_get(self, job_name_input):
        if not isinstance(job_name_input, str):
            raise ValueError('请输入正确的关键词字符串')
        self._job_name = job_name_input

    # 接收输入的工作地点
    @property
    def job_loc_cmd_get(self):
        return self._job_loc

    @job_loc_cmd_get.setter
    def job_loc_cmd_get(self, job_loc_input):
            if not isinstance(job_loc_input, str):
                raise ValueError('请输入正确的关键词字符串')
            if job_loc_input not in job_loc_list:
                print('请输入主要的城市。')
            self._job_loc = job_loc_input

    def url_cook(self):
        """
        根据输入工作信息拼接url
        :return:
        """
        url_crawl = self.url_base + 'jl=' + parse.quote(self._job_loc) \
                    + '&kw=' + parse.quote(self._job_name) + '&p={}&isadv=0'
        return url_crawl

    def html_crawl(self, url_crawl):
        """
        根据url下载网页
        :param url_crawl:
        :return:
        """
        try:
            response = request.Request(url_crawl, headers=headers)
            html_requested = request.urlopen(response)
            html_decoded = html_requested.read().decode('utf-8')
            self.html_pool.append(html_decoded)
            print('-----正在下载-----')
            sleep(3)
        except error.HTTPError as e:
            if hasattr(e, 'code'):
                print(e.code)

    def html_parse(self, html_docoded):
        """
        解析下载的html信息
        :param html_docoded:
        :return:
        """
        job_fb = []
        job_name = []
        soup = BeautifulSoup(html_docoded, 'lxml')
        # 提取工作名称
        for td_tag in soup.find_all('td', class_='zwmc')[1:]:
            sub_soup = BeautifulSoup(str(td_tag), 'lxml')
            if '</b>' in str(sub_soup.a):
                raw_name = re.findall(r'<a.+?>(.+?)?<b>(.+?)</b>(.+?)?</a>', str(td_tag.a))[0]
                job_name_fill = ''
                for name in raw_name:
                    if isinstance(name, str):
                        job_name_fill += name.strip()
                job_name.append(job_name_fill)
            else:
                job_name.append(sub_soup.a.string)
                # job_href.append(sub_soup.a.get('href'))
        # 提取反馈率
        for td_tag in soup.find_all('td', class_='fk_lv')[1:]:
            sub_soup = BeautifulSoup(str(td_tag), 'lxml')
            job_fb.append(sub_soup.span.string)
        # 提取公司名称、薪水、地点
        job_company = [td_tag.a.string for td_tag in soup.find_all('td', class_='gsmc')[1:]]
        job_salary = [td_tag.string for td_tag in soup.find_all('td', class_='zwyx')[1:]]
        job_location = [td_tag.string for td_tag in soup.find_all('td', class_='gzdd')[1:]]

        self.parse_pool.append(zip(job_name, job_fb, job_company, job_salary, job_location))

    def job_info_store(self, job_info):
        """
        将工作信息储存到数据库里
        :param job_info:
        :return:
        """
        for elem in job_info:
            self.cur.execute("insert into jobs (job_name, feedback_rate, company_name, salary, location)"
                             " values('{}', '{}', '{}', '{}', '{}')"
                             .format(str(elem[0]), str(elem[1]), str(elem[2]), str(elem[3]), str(elem[4])))
            self.conn.commit()

    def run(self):
        """
        主函数运行
        :return:
        """
        self.job_loc_cmd_get = input('请输入工作地点')
        self.job_name_cmd_get = input('请输入搜索工作名称')
        url_list = []
        html_thread_object = []

        for x in range(1, 5):
            url_list.append(self.url_cook().format(x))
            t = Thread(target=self.html_crawl, args=(url_list[x-1],), name='Crawl_Thread')
            html_thread_object.append(t)
        for elem in html_thread_object:
            elem.start()
        for elem in html_thread_object:
            elem.join()

        for num in range(0, len(self.html_pool)):
            self.html_parse(self.html_pool[num])
            print('-----网页解析完毕-----')
            self.job_info_store(self.parse_pool[num])
            print('-----数据库储存完毕-----')


if __name__ == '__main__':
    spider = ZLSpider()
    spider.run()