from random import randint
import requests
from bs4 import BeautifulSoup
import json
import os
import time

base_url = "https://suumo.jp/jj/chintai/ichiran/FR301FC001/?ar=030&bs=040&page={}"

# Ensure the directory exists
output_dir = "data/apartments_json"
os.makedirs(output_dir, exist_ok=True)


total_pages = 10_000

for page in range(1, total_pages + 1):
    print(f"Scraping page {page}/{total_pages}")
    response = requests.get(base_url.format(page))
    soup = BeautifulSoup(response.content, "html.parser")

    # Extract apartment details
    cassette_items = soup.find_all("div", class_="cassetteitem")

    for item in cassette_items:
        property_name = item.find(
            "div", class_="cassetteitem_content-title"
        ).text.strip()
        location = item.find("li", class_="cassetteitem_detail-col1").text.strip()
        stations_info = item.find("li", class_="cassetteitem_detail-col2")
        stations = [
            station.text.strip()
            for station in stations_info.find_all(
                "div", class_="cassetteitem_detail-text"
            )
            if station.text.strip() != ""
        ]

        rows = item.find_all("tr", class_="js-cassette_link")
        apartments = []
        for row in rows:
            rent = row.find("span", class_="cassetteitem_price--rent").text.strip()
            admin_fee = row.find(
                "span", class_="cassetteitem_price--administration"
            ).text.strip()
            deposit = row.find(
                "span", class_="cassetteitem_price--deposit"
            ).text.strip()
            gratuity = row.find(
                "span", class_="cassetteitem_price--gratuity"
            ).text.strip()
            madori = row.find("span", class_="cassetteitem_madori").text.strip()
            menseki = row.find("span", class_="cassetteitem_menseki").text.strip()
            apartment_url = (
                "https://suumo.jp"
                + row.find("a", class_="js-cassette_link_href")["href"]
            )
            apartments.append(
                {
                    "monthly_rent": rent,
                    "management_fee": admin_fee,
                    "deposit": deposit,  # deposit fee
                    "gratuity": gratuity,
                    "madori": madori,  # layout ie 1k, 2kdl
                    "menseki": menseki,  # area of the apartment in m2
                    "url": apartment_url,
                }
            )
        apartment_data = {
            "property_name": property_name,
            "location": location,
            "stations": stations,
            "apartment": apartments,
        }

        # Save each apartment as a JSON file
        file_name = f"{property_name}_{page}.json".replace("/", "_")
        with open(
            os.path.join(output_dir, file_name), "w", encoding="utf-8"
        ) as json_file:
            json.dump(apartment_data, json_file, ensure_ascii=False, indent=4)

    time.sleep(randint(1, 4))  # Randomly sleep between 1 - 4 seconds

print(f"Data scraping complete! JSON files saved in {output_dir} directory")
