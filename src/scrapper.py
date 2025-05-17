import requests
from bs4 import BeautifulSoup
import json
import os
import time

base_url = "https://suumo.jp/jj/chintai/ichiran/FR301FC001/?ar=030&bs=040&page={}"

# Ensure the directory exists
output_dir = "data/apartments_json"
os.makedirs(output_dir, exist_ok=True)

# Define total pages (as retrieved previously, 4133)
total_pages = 10_000
t1 = time.now()
for page in range(1, total_pages + 1):
    print(f"Scraping page {page}/{total_pages}")
    response = requests.get(base_url.format(page))
    soup = BeautifulSoup(response.content, "html.parser")

    # Extract apartment details
    cassette_items = soup.find_all("div", class_="cassetteitem")

    for item in cassette_items:
        apartment_name = item.find(
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
        stations_str = ", ".join(stations)

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
                    "Monthly Rent": rent,
                    "Management Fee": admin_fee,
                    "Shikikin (Deposit)": deposit,
                    "Reikin (Gratuity)": gratuity,
                    "Madori (Layout)": madori,
                    "Menseki (Area)": menseki,
                    "URL": apartment_url,
                }
            )
        apartment_data = {
            "Apartment Name": apartment_name,
            "Location": location,
            "Stations": stations_str,
            "Apartments": apartments,
        }

        # Save each apartment as a JSON file
        file_name = f"{apartment_name}_{page}.json".replace("/", "_")
        with open(
            os.path.join(output_dir, file_name), "w", encoding="utf-8"
        ) as json_file:
            json.dump(apartment_data, json_file, ensure_ascii=False, indent=4)


print(f"Data scraping complete! JSON files saved in {output_dir} directory")
