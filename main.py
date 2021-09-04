#!/usr/bin/python3

import threading
from logger import Logger
from scraper import *
from database import Database

logger = Logger("main")

def scrape_periodically(scrapers, period):
    logger.info(f"Starting periodic scraping.")
    for scraper in scrapers:
        try:
            scraper.start()
            scraper.time_to_scrape_event.set()
        except RuntimeError:
            scraper.time_to_scrape_event.set()
    logger.info(f"Next scraping is set to start after {period} seconds.")
    threading.Timer(period, scrape_periodically, [scrapers, period]).start()

if __name__ == "__main__":
    db = Database()
    db.start()
    locator = Locator(db)
    scrapers = [CVScraper(db, locator), CVbankasScraper(db, locator),
        CVonlineScraper(db, locator), CVmarketScraper(db, locator),
            GeraPraktikaScraper(db, locator)]
    scrape_periodically(scrapers, 60)
