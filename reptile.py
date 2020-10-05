# ！/usr/bin/env python
# —*— coding: utf-8 —*—

import requests
import random
from pymongo import MongoClient
import json, time, datetime
from bs4 import BeautifulSoup
from flask import Flask, request, jsonify, send_file
import re
import numpy as np
import threading

class DB:
    def __init__(self, usr, passwd, ip):
        # 建立连接
        self.db_connect = MongoClient('mongodb://%s:%s@%s' % (usr, passwd, ip))
        self.using_db = 0
    def clear(self):
        # 清除残留数据
        self.db_connect['DataByJob'].command("dropDatabase")
        self.db_connect['DataByCity'].command("dropDatabase")
    def save(self, db_name, col_name, save_data):
        time_start = time.time()
        self.db_connect[db_name][col_name
            .replace('.', '[point]')
            .replace(' ', '[empty]')].insert_many(save_data)
        time_stop = time.time()
        print('data saved, %s sec used' % (time_stop - time_start))
        return 0
    def load(self, db_name, col_name):
        return_list = []
        for data in self.db_connect[db_name][col_name.replace('.', '[point]')
                .replace(' ', '[empty]')
        ].find({}, {"_id": 0}):
            return_list.append(data)
        return return_list
    def search(self):
        jobs_num_per_city = {}
        company_name_count = {}
        avg_salary_per_city = {}
        educational_requirements = {}
        experiment_requirments = {}
        industry_field = {}
        # 1. 获取不同城市的招聘数量等
        for job in self.db_connect['DataByJob'].list_collection_names():
            for city_str in self.db_connect['DataByJob'][job].find({}, {"city": 1, "companyShortName": 1,
                                                                        'education': 1, 'workYear': 1,
                                                                        'industryField': 1}):
                # 1.1 不同城市的招聘数量
                if city_str['city'] not in jobs_num_per_city.keys():
                    jobs_num_per_city[city_str['city']] = 1
                else:
                    jobs_num_per_city[city_str['city']] += 1
                # 1.2 每个公司的招聘数量
                if city_str['companyShortName'] not in company_name_count.keys():
                    company_name_count[city_str['companyShortName']] = 1
                else:
                    company_name_count[city_str['companyShortName']] += 1
                # 1.3 学历要求
                if city_str['education'] not in educational_requirements.keys():
                    educational_requirements[city_str['education']] = 1
                else:
                    educational_requirements[city_str['education']] += 1
                # 1.4 工作经历要求
                if city_str['workYear'] not in experiment_requirments.keys():
                    experiment_requirments[city_str['workYear']] = 1
                else:
                    experiment_requirments[city_str['workYear']] += 1
                # 1.5 行业领域
                if city_str['industryField'] not in industry_field.keys():
                    industry_field[city_str['industryField']] = 1
                else:
                    industry_field[city_str['industryField']] += 1

        # 2. 获取每个城市的平均薪资
        for city in self.db_connect['DataByCity'].list_collection_names():
            city_name = city.replace('[point]', '.') \
                .replace('[empty]', ' ')
            salary_list = []
            for salary_str in self.db_connect['DataByCity'][city].find({}, {"salary": 1}):
                low_salary, high_salary = re.split('-', salary_str['salary'])
                salary_list.append((int(re.sub(r'\D', "", low_salary)) + int(re.sub(r'\D', "", high_salary)))/2)
            avg_salary_per_city[city_name] = np.round(np.mean(salary_list), 1)

        return_dict = {}
        return_dict['各城市岗位需求'] = jobs_num_per_city
        return_dict['各城市平均工资'] = avg_salary_per_city
        return_dict['各公司招聘数量'] = company_name_count
        return_dict['学历要求'] = educational_requirements
        return_dict['工作经验要求'] = experiment_requirments
        return_dict['行业领域'] = industry_field

        return return_dict


class GetDataByJob:
    # 初始化参数：工作列表，初始化后的数据库类
    def __init__(self, DB):
        self.DB = DB
        self.job_all, self.job_list = self.get_job_names()
        # 第一次请求的URL
        self.first_url = 'https://www.lagou.com/jobs/list_Java?px=new&city=%E5%85%A8%E5%9B%BD#order'
        # 第二次请求的URL
        self.second_url = 'https://www.lagou.com/jobs/positionAjax.json?needAddtionalResult=false'
        # 伪装请求头
        self.headers = {
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'zh-CN,zh;q=0.9',
            'Connection': 'keep-alive',
            'Content-Length': '25',
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'Host': 'www.lagou.com',
            'Origin': 'https://www.lagou.com',
            'Referer': 'https://www.lagou.com/jobs/list_Python?labelWords=&fromSearch=true&suginput=',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.97 Safari/537.36',
            'X-Anit-Forge-Code': '0',
            'X-Anit-Forge-Token': 'None',
            'X-Requested-With': 'XMLHttpRequest'
        }
    # 获取工作名称列表
    # 作者：李昶
    def get_job_names(self):
        url = "https://www.lagou.com/"
        r = requests.get(url, headers={
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.97 Safari/537.36'
        })
        r.raise_for_status()
        r.encoding = r.apparent_encoding

        html = r.text
        soup = BeautifulSoup(html, 'html.parser')
        menu_box = soup.find_all('div', class_='menu_box')
        job_all = []
        job_list_all = []
        for box_item in menu_box:
            menu_main = box_item.find('h2').text.strip()
            job_dls = box_item.find_all('dl')
            job_all_item = {}
            for job_dl in job_dls:
                job_list = []
                job_dl_span = job_dl.find('span').text
                jobs_h3_list = job_dl.find_all('h3')
                for jobs_h3 in jobs_h3_list:
                    job_list.append(jobs_h3.text)
                    job_list_all.append(jobs_h3.text)
                job_all_item[job_dl_span] = job_list
            job_all.append({menu_main: job_all_item})
        return job_all, job_list_all

    def save_to_db(self, job_name, input_list):
        self.DB.save('DataByJob', job_name, input_list)
    def save_update_time(self):
        self.DB.save('DataByJob', 'update-time', time.strftime('%Y-%m-%d %H:%M:%S'))
    def run(self):
        # 创建一个session对象
        session = requests.session()
        # 进行第一次连接获取cookie
        try:
            session.get(self.first_url, headers={
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.97 Safari/537.36'
            })
        except Exception as e:
            print(e)
            return 1

        for job in self.job_list:
            return_list = []
            # 构建请求数据
            data = {
                'first': 'true',
                'pn': '1',
                'px': 'new',
                'kd': job
            }
            # 发起第二次请求，获取数据
            try:
                result = session.post(self.second_url, headers=self.headers, data=data, allow_redirects=False)
            except Exception as e:
                print(e)
                return 1
            # 数据清洗及保存
            res = result.json()['content']['positionResult']['result']
            print('---------%s--------' % job)
            if res:
                # 有部分职业比如Perl会出现无数据的情况，这时要忽略该职业
                for r in res:
                    d = {
                        'city': r['city'],
                        'companyShortName': r['companyShortName'],
                        'workType': r['firstType'],
                        'skillLables': r['skillLables'],
                        'industryField': r['industryField'],
                        'education': r['education'],
                        'positionName': r['positionName'],
                        'salary': r['salary'],
                        'workYear': r['workYear'],
                        'createTime': r['createTime']
                    }
                    return_list.append(d)
                self.save_to_db(job, return_list)
            # 延时，如果去掉可能会被封ip
            time.sleep(5 + 5 * random.random())
        session.close()
        return 0


class GetDataByCity:
    def __init__(self, DB):
        self.DB = DB
        self.city_list = self.get_city_names()
        self.first_url = 'https://www.lagou.com/jobs/list_Java?px=new&city=%E5%85%A8%E5%9B%BD#order'
        self.second_url = 'https://www.lagou.com/jobs/positionAjax.json?needAddtionalResult=false'
        self.headers = {
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'zh-CN,zh;q=0.9',
            'Connection': 'keep-alive',
            'Content-Length': '25',
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'Host': 'www.lagou.com',
            'Origin': 'https://www.lagou.com',
            'Referer': 'https://www.lagou.com/jobs/list_Python?labelWords=&fromSearch=true&suginput=',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.97 Safari/537.36',
            'X-Anit-Forge-Code': '0',
            'X-Anit-Forge-Token': 'None',
            'X-Requested-With': 'XMLHttpRequest'
        }
    # 获取所有城市名称
    def get_city_names(self):
        url = "https://www.lagou.com/jobs/allCity.html?keyword=Java&px=new&city=%E5%85%A8%E5%9B%BD&positionNum=500+&companyNum=0&isCompanySelected=false&labelWords="
        r = requests.get(url, headers={
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.97 Safari/537.36'
        })
        r.raise_for_status()
        r.encoding = r.apparent_encoding

        html = r.text
        soup = BeautifulSoup(html, 'html.parser')
        menu_box = soup.find_all('table', class_='word_list')
        city_list = []
        for box_item in menu_box:
            city_li = box_item.find_all('li')
            for city_str in city_li:
                city_list.append(city_str.find('a').text)
        return(city_list)
    def save_to_db(self, city_name, input_list):
        self.DB.save('DataByCity', city_name, input_list)
    def run(self):
        session = requests.session()
        try:
            session.get(self.first_url, headers={
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.97 Safari/537.36'
            })
        except Exception as e:
            print(e)
            return 1

        for city in self.city_list:
            return_list = []
            data = {
                'first': 'true',
                'pn': '1',
                'px': 'new',
                'city': city
            }
            try:
                result = session.post(self.second_url, headers=self.headers, data=data, allow_redirects=False)
            except Exception as e:
                print(e)
                return 1

            res = result.json()['content']['positionResult']['result']
            print('---------%s--------'%city)
            if res:
                for r in res:
                    d = {
                        'companyShortName': r['companyShortName'],
                        'workType': r['firstType'],
                        'skillLables': r['skillLables'],
                        'industryField': r['industryField'],
                        'education': r['education'],
                        'positionName': r['positionName'],
                        'salary': r['salary'],
                        'workYear': r['workYear'],
                        'createTime': r['createTime']
                    }
                    return_list.append(d)
                self.save_to_db(city, return_list)
            time.sleep(5 + 5 * random.random())
        session.close()
        return 0


# 重定义datetime格式的编码方法
# 如果没有这一段，则会出现错误：TypeError: Object of type datetime is not JSON serializable
class DateEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.strftime("%Y-%m-%d %H:%M:%S")
        else:
            return json.JSONEncoder.default(self, obj)

class LocalAPI(threading.Thread):
    def __init__(self, DB):
        threading.Thread.__init__(self)
        self.DB = DB

    def run(self):
        time.sleep(10)
        app = Flask(__name__)
        MY_URL = '/2-204/'
        # 获取：城市数据，职业数据，等等等等
        @app.route(MY_URL + 'request-all', endpoint='request-all', methods=['GET'])
        def request_all():
            print('request received')
            if self.DB.using_db == 1:
                return_dict = {'msg': '数据库更新中，请稍后再试'}
                return json.dumps(return_dict, cls=DateEncoder, ensure_ascii=False)
            else:
                return_dict = self.DB.search()
                for i in return_dict:
                    print('%s : %s'%(i, return_dict[i]))
                return json.dumps(return_dict, cls=DateEncoder, ensure_ascii=False)
        app.run(threaded=True, processes=True, host='0.0.0.0', port=5000, debug=True, use_reloader=False)

class MainExecution(threading.Thread):
    def __init__(self, DB):
        threading.Thread.__init__(self)
        self.DB = DB
    def run(self):
        running_first_time = 1
        while 1:
            if running_first_time:
                self.get_data()
                running_first_time = 0
                time.sleep(100)
            else:
                # 每天上午10点和晚上10点获取数据
                T = int(time.strftime('%H'))
                if T == 10 or T == 22:
                    self.get_data()
                time.sleep(3600)
    def get_data(self):
        # 加锁
        self.DB.using_db = 1
        # 清空数据库
        self.DB.clear()
        # 获取职业数据
        GDBJ = GetDataByJob(self.DB)
        GDBJ.run()
        # 获取城市数据
        GDBC = GetDataByCity(self.DB)
        GDBC.run()
        # 解锁
        self.DB.using_db = 0


if __name__ == '__main__':
    usr = '********'
    passwd = '******************************************'
    ip = 'xxx.xxx.xxx.xxx'

    DataBase = DB(usr, passwd, ip)

    thread_MainExecution = MainExecution(DataBase)
    thread_LocalAPI = LocalAPI(DataBase)

    thread_MainExecution.start()
    thread_LocalAPI.start()

