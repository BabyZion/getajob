#!/usr/bin/python3

from geopy.distance import geodesic
from geopy.extra.rate_limiter import RateLimiter
from geopy.geocoders import Nominatim
from logger import Logger


class Locator:

    def __init__(self, db):
        self.geocoder = RateLimiter(Nominatim(user_agent='CVscraper').geocode, min_delay_seconds=1)
        self.geodesic = RateLimiter(geodesic, min_delay_seconds=1)
        self.db = db
        self.logger = Logger('Locator')
        self.logger.info(f"Locator object created.")

    def distance_between_addresses(self, addr1, addr2):
        self.logger.info(f"Calculating distance between {addr1} and {addr2}.")
        distance = None
        cord_1 = self.__get_coordinates_from_address(addr1)
        cord_2 = self.__get_coordinates_from_address(addr2)
        if cord_1 != (0,0) or cord_2 != (0,0):
            distance = round(self.geodesic(cord_1, cord_2).km, 3)
        if distance:
            self.logger.info(f"Distance between {addr1} and {addr2} is {distance}.")
        else:
            self.logger.warning(f"Distance between {addr1} and {addr2} COULDN'T BE CALCULATED!!!")
        return distance

    def TG3_distance(self, address):
        req = f"SELECT dist_to_tg3 FROM addresses WHERE name='{address}';"
        try:
            tg3_dist = self.db.request(req)[0][0]
        except IndexError:
            self.logger.warning(f"{address} seems to not be in a database.")
            tg3_dist = None
        if not tg3_dist:
            tg3_dist = self.distance_between_addresses(address, "Tuskulenu g. 3, Vilnius")
            self.logger.info(f"Updating TG3 distance item in database of {address}")
            req = f"UPDATE addresses SET dist_to_tg3={tg3_dist} WHERE name='{address}';"
            self.db.request(req, fetch=False)
        else:
            self.logger.info(f"Found TG3 distance of {address} in the database - {tg3_dist}.")
        return tg3_dist

    def __get_addr_from_db(self, addr):
        address = {}
        req = f"SELECT EXISTS(SELECT 1 FROM addresses WHERE name='{addr}');"
        try:
            exists = self.db.request(req)[0][0]
        except TypeError as e:
            self.logger.error(f"Problem querying {addr} from the database - {e}")
            exists = None
        if exists:
            self.logger.info(f"{addr} exists in a database.")
            req = f"SELECT * FROM addresses WHERE name='{addr}';"
            address_db = self.db.request(req)[0]
            address['name'] = address_db[0]
            address['id_osm'] = address_db[1]
            address['lat'] = address_db[2]
            address['lon'] = address_db[3]
            address['dist_to_TG3'] = address_db[4]
        return address

    def __get_coordinates_from_address(self, address):
        # First check if address already exists in a database.
        db_data = self.__get_addr_from_db(address)
        if db_data:
            latitude = db_data['lat']
            longitude = db_data['lon']
        else:
            # Check the OSM database.
            try:
                self.logger.info(f"Trying to fetch {address} from OSM database.")
                osm_data = self.geocoder(address).raw
                osm_data['name'] = address
                latitude = osm_data['lat']
                longitude = osm_data['lon']
                self.logger.info(f"Found {address} in OSM database successfully.")
                self.__osm_data_to_db(osm_data)
            except AttributeError:
                self.logger.error(f"Couldn't fetch {address} from OSM database.")
                osm_data = {}
                osm_data['name'] = address
                osm_data['lon'] = 0
                osm_data['lat'] = 0
                osm_data['place_id'] = 0
                self.__osm_data_to_db(osm_data)
                return (0,0)
        return (latitude, longitude)

    def __osm_data_to_db(self, osm_data):
        data = {}
        data['name'] = osm_data['name']
        data['id_osm'] = osm_data['place_id']
        data['lat'] = osm_data['lat']
        data['lon'] = osm_data['lon']
        self.db.queue.put(('addresses', data))
        self.logger.info(f"Put {osm_data['name']} into database queue.")
