#!/usr/bin/python3

import requests
import json
import threading
from bs4 import BeautifulSoup


class Scraper(threading.Thread):

    def __init__(self):
        super().__init__()

    def get_page_data(self, link):
        req = requests.get(link).text
        return req

    def get_job_data(self, link):
        data = self.get_page_data(link)
        data = json.loads(data)
        return data

    def refine_job_ad_data(self):
        pass

    def refine_job_data(self):
        pass

    def build_request_link(self, key_words, no_of_jobs=1):
        request_link = self.base_search_link
        for key_word in key_words:
            request_link += self.key_word_req + key_word
        request_link += self.page_size_req + str(no_of_jobs)
        return request_link

    def get_number_of_ads(self, page, f_class, s_class):
        soup = BeautifulSoup(page, 'lxml')
        try:
            no_of_pages = int(soup.find(class_=f_class).find_all(s_class)[-1].text)
        except AttributeError:
            no_of_pages = 1
        return no_of_pages

    def count_salary(self, salary_string):
        salFrom = salTo = salAvg = 0
        if "nuo" in salary_string.lower():
            sal_s = salary_string.split()
            salFrom = int("".join(filter(str.isdigit, sal_s[1])))
        if "iki" in salary_string.lower():
            sal_s = salary_string.split()
            salTo = int("".join(filter(str.isdigit, sal_s[1])))
        if "-" in salary_string.lower():
            sal_s = salary_string.split('-')
            salFrom = int("".join(filter(str.isdigit, sal_s[0])))
            salTo = int("".join(filter(str.isdigit, sal_s[1])))
            salAvg = (salFrom + salTo) / 2
        return salFrom, salTo, salAvg

    def run(self):
        link = self.build_request_link(['linux'])
        try:
            no_of_ads = self.get_number_of_ads(self.get_job_data(link))
            link = self.build_request_link(['python'], no_of_ads)
        except TypeError:
            pass
        jobs = self.get_job_data(link)
        ref_jobs = self.refine_job_data(jobs)
        for job in ref_jobs:
            print(job)
            print()


class CVScraper(Scraper):
    
    def __init__(self):
        super().__init__()
        self.base_link = "https://www.cv.lt"
        self.base_search_link = "https://www.cv.lt/smvc/board/list/get?desired=false&handicapped=false&page=1&remote=false&sortField=ORDER_TIME"
        self.page_size_req = "&pageSize="
        self.key_word_req = "&texts="

    def get_number_of_ads(self, page):
        no_of_jobs = page['searchResult']['rowCount'] + 1
        return no_of_jobs

    def refine_job_data(self, data):
        jobs = []
        for job in data['searchResult']['results']:
            job_data = {}
            job_data['title'] = job['title']
            job_data['company'] = job['company']
            job_data['city'] = job['cities']
            job_data['salaryFrom'], job_data['salaryTo'], job_data['salaryAvg'] = self.count_salary(job['salary'])
            job_data['tags'] = job['firstDepartmentName'].split()
            job_data['url'] = self.base_link + job['url']
            jobs.append(job_data)
        return jobs

    def refine_job_ad_data(self, link):
        req = self.get_page_data(link)
        soup = BeautifulSoup(req, 'lxml')
        description = soup.find(class_="content job-description")
        # To be implemented...


class CVbankasScraper(Scraper):
    
    def __init__(self):
        super().__init__()
        self.base_link = "https://www.cvbankas.lt"
        self.base_search_link = "https://www.cvbankas.lt/?"
        self.key_word_req = "&keyw="
        self.no_of_pages_req = "&page="
        self.page_line_f_class = "pages_ul_inner"
        self.page_line_s_class = "a"

    def build_request_link(self, keywords, no_of_pages=1):
        links = []
        for keyword in keywords:
            link = self.base_search_link + self.key_word_req + keyword
            links.append(link)
            no_of_pages = self.get_number_of_ads(self.get_page_data(link), self.page_line_f_class, self.page_line_s_class)
            for i in range (2, no_of_pages + 1):
                page_link = link + self.no_of_pages_req + str(i)
                links.append(page_link)
        return links
            
    def get_job_data(self, links):
        all_jobs = []
        for link in links:
            req = self.get_page_data(link)
            soup = BeautifulSoup(req, 'lxml')
            jobs = soup.find_all("a", class_="list_a can_visited list_a_has_logo")
            all_jobs += jobs
        return all_jobs

    def refine_job_data(self, data):
        jobs = []
        for job in data:
            if not job: continue
            # job = BeautifulSoup(job, 'lxml')
            job_data = {}
            job_data['title'] = job.h3.text
            job_data['company'] = job.find(class_="dib mt5").text
            job_data['city'] = job.find(class_="list_city").text
            job_data['tags'] = None
            try:
                salary_str = job.find(class_="salary_amount").text
                job_data['salaryFrom'], job_data['salaryTo'], job_data['salaryAvg'] = self.count_salary(salary_str)
            except AttributeError:
                job_data['salaryFrom'] = job_data['salaryTo'] = job_data['salaryAvg'] = 0
            job_data['url'] = job['href']
            jobs.append(job_data)
        return jobs


class CVonlineScraper(Scraper):
    
    def __init__(self):
        super().__init__()
        self.base_link = "https://www.cvonline.lt"
        self.base_search_link = "https://www.cvonline.lt/api/v1/vacancies-service/search?&offset=0&isHourlySalary=false&isRemoteWork=false&lang=lt"
        self.page_size_req = "&limit="
        self.key_word_req = "&keywords[]="

    def get_number_of_ads(self, page):
        no_of_jobs = page['total']
        return no_of_jobs

    def refine_job_data(self, data):
        jobs = []
        for job in data["vacancies"]:
            job_data = {}
            job_data['title'] = job['positionTitle']
            job_data['company'] = job['employerName']
            job_data['city'] = job['townId']
            job_data['tags'] = job['keywords']
            job_data['salaryFrom'] = job['salaryFrom']
            job_data['salaryTo'] = job['salaryTo']
            if not job_data['salaryTo']: job_data['salaryTo'] = 0
            if job_data['salaryFrom'] and job_data['salaryTo']:
                job_data['salaryAvg'] = (job_data['salaryFrom'] + job_data['salaryTo']) / 2
            else:
                job_data['salaryAvg'] = 0
            job_data['url'] = self.base_link + '/lt/vacancy/' + str(job['id'])
            jobs.append(job_data)
        return jobs

    def refine_job_ad_data(self, link):
        req = self.get_page_data(link)
        soup = BeautifulSoup(req, 'lxml')
        description = soup.find(class_="react-tabs__tab-panel react-tabs__tab-panel--selected")
        # To be implemented...


class CVmarketScraper(Scraper):

    def __init__(self):
        super().__init__()
        self.base_link = "https://www.cvmarket.lt"
        self.base_search_link = "https://www.cvmarket.lt/joboffers.php?_track=index_click_job_search&op=search&search_location=landingpage&ga_track=homepage"
        self.key_word_req = "&search[keyword]="
        self.no_of_pages_req = "&start="

    def get_number_of_ads(self, page):
        soup = BeautifulSoup(page, 'lxml')
        try:
            no_of_pages = int(soup.find(class_="pager_xs pagination").find_all("li", class_="")[-1].text)
        except AttributeError:
            no_of_pages = 1
        return no_of_pages

    def build_request_link(self, keywords, no_of_pages=1):
        links = []
        for keyword in keywords:
            link = self.base_search_link + self.key_word_req + keyword
            links.append(link)
            no_of_pages = self.get_number_of_ads(self.get_page_data(link))
            for i in range (2, no_of_pages + 1):
                page_link = link + self.no_of_pages_req + str((i*30) - 30)
                links.append(page_link)
        return links

    def get_job_data(self, links):
        all_jobs = []
        for link in links:
            req = self.get_page_data(link)
            soup = BeautifulSoup(req, 'lxml')
            jobs = soup.find_all(class_="f_job_row2")
            all_jobs += jobs
        return all_jobs

    def refine_job_data(self, data):
        jobs = []
        for job in data:
            job_data = {}
            job_data['title'] = job.find(class_="f_job_title main_job_link limited-lines").text
            job_data['company'] = job.find(class_="f_job_company").text
            job_data['city'] = job.find(class_="f_job_city").text.strip()
            job_data['tags'] = None
            try:
                salary_str = job.find(class_="f_job_salary").text
                job_data['salaryFrom'], job_data['salaryTo'], job_data['salaryAvg'] = self.count_salary(salary_str)
            except AttributeError:
                job_data['salaryFrom'] = job_data['salaryTo'] = job_data['salaryAvg'] = 0
            job_data['url'] = self.base_link + job.find(class_="f_job_title main_job_link limited-lines")['href']
            jobs.append(job_data)
        return jobs


class GeraPraktikaScraper(Scraper):
    
    def __init__(self):
        super().__init__()
        self.base_link = "https://www.gerapraktika.lt"
        self.base_search_link = "https://www.gerapraktika.lt/praktikos-skelbimai/p0?"
        self.key_word_req = ";title="
        self.no_of_pages_req = "/p"
        self.page_line_f_class = "pager"
        self.page_line_s_class = "invisible_pager_button"

    def build_request_link(self, keywords, no_of_pages=1):
        links = []
        for keyword in keywords:
            link = self.base_search_link + self.key_word_req + keyword
            links.append(link)
            no_of_pages = self.get_number_of_ads(self.get_page_data(link), self.page_line_f_class, self.page_line_s_class)
            for i in range (2, no_of_pages + 1):
                page_tag = f"/p{i*20-20}?"
                page_link = link.replace("/p0?", page_tag)
                links.append(page_link)
        return links

    def get_job_data(self, links):
        all_jobs = []
        for link in links:
            req = self.get_page_data(link)
            soup = BeautifulSoup(req, 'lxml')
            jobs = soup.find_all(class_="announcement")
            all_jobs += jobs
        return all_jobs

    def refine_job_data(self, data):
        jobs = []
        for job in data:
            job_data = {}
            job_data['title'] = job.find(class_="item_title").text
            job_data['company'] = job.find(class_="company_url").text
            job_data['city'] = job.find(class_="location").text
            try:
                job_data['tags'] = job.find(class_="area_title").text
            except AttributeError:
                job_data['tags'] = None
            job_data['salaryFrom'] = job_data['salaryTo'] = job_data['salaryAvg'] = 0
            job_data['url'] = self.base_link + job.find(class_="company_title")['href']
            jobs.append(job_data)
        return jobs
