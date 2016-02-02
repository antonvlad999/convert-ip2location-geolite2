Description
===========

Converts IP2Location LITE DB1 into MaxMind GeoLite2 Country data

OR

Converts IP2Location LITE DB11 into MaxMind GeoLite2 City data


Usage
=====

perl convert.pl IP2LOCATION-LITE-DB1.CSV

OR

perl convert.pl IP2LOCATION-LITE-DB11.CSV


Note
====

Continent, geonames id, confidence, accuracy radius, metro code, time zone, registered country, and represented country fields are not supported.
Only English names are supported at present. 

Author: Антон Владимиревич <anton.vladimir@europe.com>


Disclaimer
==========

IP2Location and Maxmind are trademark of respective owners. The IP2Location and GeoLite2 data structure is based on the public specification published by the respective owners.
