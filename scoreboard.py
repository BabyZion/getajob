#!/usr/bin/python3

class Scoreboard:

    def __init__(self, db):
        self.db = db

    def todays_best(self):
        # Returns up to 5 best combined score job offers found during last 24 h.
        # Returns job offers that are at least TOP 30 (?) all time.
        pass

    def top_offers(self):
        # Returns up to 5 best offers of all time.
        pass

    def best_new_offer(self):
        # Returns best found new offer after job search.
        # Must be at least TOP 30 (?).
        pass
