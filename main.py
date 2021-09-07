#!/usr/bin/python3

from datetime import datetime
from logger import Logger
from scraper import *
from database import Database
from scoreboard import Scoreboard

logger = Logger("main")

def scrape_periodically(scrapers, period, sb):
    logger.info(f"Starting periodic scraping.")
    sb.date_of_last_scrape = datetime.now()
    for scraper in scrapers:
        try:
            scraper.start()
            scraper.time_to_scrape_event.set()
        except RuntimeError:
            scraper.time_to_scrape_event.set()
    logger.info(f"Next scraping is set to start after {period} seconds.")
    threading.Timer(period, scrape_periodically, [scrapers, period, sb]).start()
    for scraper in scrapers:
        scraper.finished_scraping.wait()
    sb.info_event.set()
    for scraper in scrapers:
        scraper.finished_scraping.clear()

if __name__ == "__main__":
    db = Database()
    db.start()
    sb = Scoreboard(db)
    locator = Locator(db)
    scrapers = [CVScraper(db, locator), CVbankasScraper(db, locator),
        CVonlineScraper(db, locator), CVmarketScraper(db, locator),
            GeraPraktikaScraper(db, locator)]
    scrape_periodically(scrapers, 28800, sb)
    sb.start()
