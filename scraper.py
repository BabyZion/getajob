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
        elif "iki" in salary_string.lower():
            sal_s = salary_string.split()
            salTo = int("".join(filter(str.isdigit, sal_s[1])))
        elif "-" in salary_string.lower():
            sal_s = salary_string.split('-')
            salFrom = int("".join(filter(str.isdigit, sal_s[0])))
            salTo = int("".join(filter(str.isdigit, sal_s[1])))
            salAvg = (salFrom + salTo) / 2
        else:
            salFrom = salTo = salAvg = int(salary_string)
        return salFrom, salTo, salAvg

    def gross_or_net(self, type_str):
        type_str = str(type_str).lower()
        j_type = None
        if "gross" in type_str or "bruto" in type_str or "mokes" in type_str:
            j_type = 'gross'
        if "net" in type_str or "rank" in type_str:
            j_type = 'net'
        return j_type

    def run(self):
        link = self.build_request_link(['network'])
        try:
            no_of_ads = self.get_number_of_ads(self.get_job_data(link))
            link = self.build_request_link(['network'], no_of_ads)
        except TypeError:
            pass
        jobs = self.get_job_data(link)
        ref_jobs = self.refine_job_data(jobs)
        for job in ref_jobs:
            job_ad_data = self.refine_job_ad_data(job['url'])
            job.update(job_ad_data)
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
        email_k_words = ('email', 'e-mail', 'el. paštas')
        phone_k_words = ('phone', 'telefonas')
        address_k_words = ('address', 'adresas')
        link_k_words = ('tinklalapio adresas', 'link')
        salary_k_words = ('salary', 'atlyginimas')
        remote_k_words = ('remote', 'nuotolinis')
        job_data = {}
        try:
            details_contact, details_job = soup.find_all(class_="details")
            details_contact = details_contact.find_all(class_="details-item")
            for detail in details_contact:
                detail_h6 = str(detail.h6).lower()
                for k_word in email_k_words:
                    if k_word in detail_h6:
                        mail_fnc = str(detail.p.script).split("makeMailTrackLink")[1].split(";")[0]
                        for ch in ("()' "):
                            mail_fnc = mail_fnc.replace(ch, "")
                        mail_fnc = mail_fnc.split(',')
                        mail = mail_fnc[0] + "@" + mail_fnc[1]
                        job_data['email'] = mail
                        break
                for k_word in phone_k_words:
                    if k_word in detail_h6:
                        phone_no = str(detail.p.script).split("makePhoneTrackLink")[1].split(",")[1].replace("'", '').strip()
                        job_data['phone_no'] = phone_no
                        break
                for k_word in address_k_words:
                    if k_word in detail_h6:
                        address = str(detail.p).replace("<p>", "").replace("</p>", "")
                        job_data['address'] = address
                        break
                for k_word in link_k_words:
                    if k_word in detail_h6:
                        job_link = detail.p.a['href']
                        job_data['link'] = job_link
                        break
        except ValueError:
            details_job = soup.find(class_="details")
        for detail in details_job:
            detail_h6 = str(detail.find("h6")).lower()
            for k_word in salary_k_words:
                if k_word in detail_h6:
                    job_data['salaryType'] = self.gross_or_net(detail_h6)
                    break
            for k_word in remote_k_words:
                if k_word in detail_h6:
                    remote_detail = str(detail.p).lower()
                    if 'yes' in remote_detail or 'taip' in remote_detail:
                        job_data['remote'] = True
                    if 'no' in remote_detail or 'ne' in remote_detail:
                        job_data['remote'] = False
        return job_data
        

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

    def refine_job_ad_data(self, link):
        req = self.get_page_data(link)
        soup = BeautifulSoup(req, 'lxml')
        description = soup.find(class_="content job-description")
        job_data = {}
        try:
            salType_str = soup.find(class_="salary_calculation").text
            job_data['salaryType'] = self.gross_or_net(salType_str)
        except AttributeError:
            job_data['salaryType'] = None
        city = str(soup.find(itemprop="addressLocality").text)
        if "home" in city:
            job_data['city'] = None
            job_data['remote'] = True
        else:
            job_data['city'] = city
        try:
            job_data['link'] = str(soup.find(id="jobad_company_description").a['href'])
        except TypeError:
            job_data['link'] = None
        try:
            job_data['address'] = str(soup.find(class_="partners_company_info_additional_info_location_url").text)
        except AttributeError:
            job_data['address'] = None
        return job_data


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
        description = soup.find(class_="content job-description")
        soup = str(soup.find(type="application/json"))
        soup = soup.replace('</script>', '')
        soup = soup.replace('<script id="__NEXT_DATA__" type="application/json">', '')
        soup = json.loads(soup)
        job_data = {}
        id_ = link.split('/')[5]
        salType_str = str(soup['props']['initialReduxState']['intl']['messages']['vacancy.highlights.form.salary'])
        job_data['salaryType'] = self.gross_or_net(salType_str)
        job_data['email'] = str(soup['props']['initialReduxState']['publicVacancies'][id_]['contacts']['email'])
        job_data['phone_no'] = str(soup['props']['initialReduxState']['publicVacancies'][id_]['contacts']['phone'])
        job_data['remote'] = bool(soup['props']['initialReduxState']['publicVacancies'][id_]['highlights']['remoteWork'])
        job_data['address'] = str(soup['props']['initialReduxState']['publicVacancies'][id_]['highlights']['address'])
        job_data['link'] = soup['props']['initialReduxState']['publicVacancies'][id_]['employer']['webpageUrl']
        return job_data

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

    def refine_job_ad_data(self, link):
        req = self.get_page_data(link)
        soup = BeautifulSoup(req, 'lxml')
        description = soup.find(class_="content job-description")
        # To be implemented...


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

    def refine_job_ad_data(self, link):
        req = self.get_page_data(link)
        soup = BeautifulSoup(req, 'lxml')
        description = soup.find(class_="content job-description")
        # To be implemented...
