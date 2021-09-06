#!/usr/bin/python3

import json
import threading
from datetime import datetime, timedelta


class Scoreboard(threading.Thread):

    def __init__(self, db):
        super().__init__()
        self.db = db
        self.date_of_last_scrape = None
        self.running = False
        self.info_event = threading.Event()

    def todays_best(self):
        # Returns up to 5 best combined score job offers found during last 24 h.
        # Returns job offers that are at least TOP 30 (?) all time.
        top_req = ("SELECT combined_score FROM job_listings ORDER BY "
            "combined_score DESC OFFSET 29 LIMIT 1")
        req = (f"SELECT * FROM job_listings WHERE entered >= "
            f"current_date::timestamp and entered < current_date::timestamp "
            f"+ interval '1 Day' AND ({top_req}) < combined_score ORDER BY "
            f"combined_score DESC LIMIT 5;")
        return self.__get_offers(req)

    def top_offers(self):
        # Returns up to 5 best offers of all time.
        req = "SELECT * FROM job_listings ORDER BY combined_score DESC LIMIT 5;"
        return self.__get_offers(req)

    def best_new_offers(self):
        # Returns best found new offer after job search.
        # Must be at least TOP 30 (?).
        if self.date_of_last_scrape:
            top_req = ("SELECT combined_score FROM job_listings ORDER BY "
                "combined_score DESC OFFSET 29 LIMIT 1")
            req = (f"SELECT * FROM job_listings WHERE entered >= "
                f"'{self.date_of_last_scrape}'::timestamp "
                f"AND ({top_req}) < combined_score ORDER BY "
                f"combined_score DESC LIMIT 5;")
            return self.__get_offers(req)

    def __get_offers(self, req):
        offers = self.db.request(req, dict_cursor=True)
        # Will convert database results to python dictionary.
        # Results contain datetime object that will be converted to str by
        # "default=str" parameter since datetime objects are not serializable
        # by JSON. 
        offers = json.dumps(offers, default=str)
        return offers

    def __get_info_date(self, days=1):
        now = datetime.now()
        if now.hour >= 22:
            t_time = (now + timedelta(days=days)).replace(hour=22, minute=0, second=0)
        else:
            t_time = now.replace(hour=22, minute=0, second=0)
        return t_time

    def __periodic_daily_info(self, get_info=True):
        self.todays_best_list = self.todays_best()
        date_to_provide_info = self.__get_info_date()
        time_to_info = (date_to_provide_info - datetime.now()).total_seconds()
        threading.Timer(time_to_info, self.__periodic_daily_info)

    def __periodic_weekly_info(self, get_info=True):
        self.top_offers_list = self.top_offers()
        date_to_provide_info = self.__get_info_date(days=7)
        time_to_info = (date_to_provide_info - datetime.now()).total_seconds()
        threading.Timer(time_to_info, self.__periodic_weekly_info)

    def run(self):
        self.__periodic_daily_info(get_info=False)
        self.__periodic_weekly_info(get_info=False)
        self.running = True
        while self.running:
            self.info_event.wait()
            print(self.best_new_offers())
            self.info_event.clear()


