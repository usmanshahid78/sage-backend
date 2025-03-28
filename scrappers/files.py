import requests
from bs4 import BeautifulSoup
import csv

# URL of the permits page
url = "https://dial.deschutes.org/Real/Permits/131527"

# Send a request to the website
headers = {"User-Agent": "Mozilla/5.0"}
response = requests.get(url, headers=headers)

# Check if the request was successful
if response.status_code == 200:
    soup = BeautifulSoup(response.text, "html.parser")

    # Find the permits table
    table = soup.find("table")

    # Extract table headers
    headers = [th.text.strip() for th in table.find_all("th")]

    # Extract table rows
    rows = []
    for tr in table.find_all("tr")[1:]:  # Skip the header row
        cells = [td.text.strip() for td in tr.find_all("td")]
        if cells:
            rows.append(cells)

    # Save to CSV file
    with open("permits.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(rows)

    print("Permits data saved to permits.csv")

else:
    print(f"Failed to retrieve the page. Status code: {response.status_code}")
