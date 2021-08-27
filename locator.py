#!/usr/bin/python3

from geopy.distance import geodesic
from geopy.extra.rate_limiter import RateLimiter
from geopy.geocoders import Nominatim


class Locator:

    def __init__(self):
        self.geocoder = RateLimiter(Nominatim(user_agent='CVscraper').geocode, min_delay_seconds=1)
    
    def distance_between_addresses(self, addr1, addr2):
        cord_1 = self.__get_coordinates_from_address(addr1)
        cord_2 = self.__get_coordinates_from_address(addr2)
        if cord_1 != (0,0) and cord_2 != (0,0):
            distance = round(geodesic(cord_1, cord_2).km, 3)
        else:
            distance = None
        return distance

    def __get_coordinates_from_address(self, address):
        try:
            raw_data = self.geocoder(address).raw
        except AttributeError:
            return (0,0)
        latitude = raw_data['lat']
        longitude = raw_data['lon']
        return (latitude, longitude)

print(Locator().distance_between_addresses("Tuskulenu g. 3", "Mėnulio st. 7"))