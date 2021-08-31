#!/usr/bin/python3

from geopy.distance import geodesic
from geopy.extra.rate_limiter import RateLimiter
from geopy.geocoders import Nominatim


class Locator:

    def __init__(self, db):
        self.geocoder = RateLimiter(Nominatim(user_agent='CVscraper').geocode, min_delay_seconds=1)
        self.db = db

    def distance_between_addresses(self, addr1, addr2):
        cord_1 = self.__get_coordinates_from_address(addr1)
        cord_2 = self.__get_coordinates_from_address(addr2)
        if cord_1 != (0,0) or cord_2 != (0,0):
            distance = round(geodesic(cord_1, cord_2).km, 3)
        return distance

    def __get_addr_from_db(self, addr):
        address = {}
        req = f"SELECT EXISTS(SELECT 1 FROM addresses WHERE name='{addr}');"
        exists = self.db.request(req)[0][0]
        if exists:
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
                osm_data = self.geocoder(address).raw
                osm_data['name'] = address
                latitude = osm_data['lat']
                longitude = osm_data['lon']
                self.__osm_data_to_db(osm_data)
            except AttributeError:
                return (0,0)
        return (latitude, longitude)

    def __osm_data_to_db(self, osm_data):
        data = {}
        data['name'] = osm_data['name']
        data['id_osm'] = osm_data['place_id']
        data['lat'] = osm_data['lat']
        data['lon'] = osm_data['lon']
        self.db.queue.put(('addresses', data))
