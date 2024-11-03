import re

import pytz

WFS_BASE_URL = "https://mapy.geoportal.gov.pl/wss/service/PZGIK/ORTO/WFS/Skorowidze"
re_four_digits = re.compile(r"\d{4}")
tz = pytz.timezone("Europe/Warsaw")
crs_names_mapping = {
    "PL-1992": "EPSG:2180",
    "PL-2000:S5": "EPSG:2176",
    "PL-2000:S6": "EPSG:2177",
    "PL-2000:S7": "EPSG:2178",
    "PL-2000:S8": "EPSG:2179",
}
image_type_mapping = {
    "B/W": "Odcienie szarości",
    "RGB": "Kolor",
    "CIR": "Bliska podczerwień",
}
