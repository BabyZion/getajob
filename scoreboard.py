#!/usr/bin/python3

import json


class Scoreboard:

    def __init__(self, db):
        self.db = db
        self.date_of_last_scrape = None

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
