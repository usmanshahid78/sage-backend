import requests
from geopy.distance import geodesic
from PIL import Image
from io import BytesIO
import cv2
import numpy as np

API_KEY = "AIzaSyBWSO84ehJ8AHQwi0hHqLn5aE6bFWSC0tI"

point1 = (44.135736, -121.409958)
offset_distance = 5 / 5280
point2 = geodesic(miles=offset_distance).destination(point1, bearing=0)
point2 = (point2.latitude, point2.longitude)

def get_elevation(lat, lon):
    url = f"https://maps.googleapis.com/maps/api/elevation/json?locations={lat},{lon}&key={API_KEY}"
    response = requests.get(url)
    response_json = response.json()
    
    if "results" in response_json and response_json["results"]:
        elevation_meters = response_json["results"][0]["elevation"]
        elevation_feet = elevation_meters * 3.281
        return elevation_feet
    return None

def get_trees_data(lat, lon):
    map_url = f"https://maps.googleapis.com/maps/api/staticmap?center={lat},{lon}&zoom=18&size=600x400&maptype=satellite&key={API_KEY}"
    response = requests.get(map_url)

    if response.status_code == 200:
        img_pil = Image.open(BytesIO(response.content))
        img_cv = np.array(img_pil)
        img_cv = cv2.cvtColor(img_cv, cv2.COLOR_RGB2BGR)
        gray = cv2.cvtColor(img_cv, cv2.COLOR_BGR2GRAY)
        vegetation_mask = gray > 100  # Adjust this threshold if needed
        green_percentage = (np.sum(vegetation_mask) / vegetation_mask.size) * 100

        if green_percentage > 5:
            print("Trees/Brush: YES")
        else:
            print("Trees/Brush: NO")
    else:
        print("Error fetching the map image.")

def get_building_data(lat, lon):
    map_url = f"https://maps.googleapis.com/maps/api/staticmap?center={lat},{lon}&zoom=18&size=600x400&maptype=satellite&key={API_KEY}"
    response = requests.get(map_url)
    if response.status_code == 200:
        img_pil = Image.open(BytesIO(response.content))
        img_cv = np.array(img_pil)
        img_cv = cv2.cvtColor(img_cv, cv2.COLOR_RGB2BGR)
        gray = cv2.cvtColor(img_cv, cv2.COLOR_BGR2GRAY)
        edges = cv2.Canny(gray, 50, 150)
        contours, _ = cv2.findContours(edges, cv2.RETR_EXTERNAL, cv2.CHAIN_APPROX_SIMPLE)
        building_contours = [cnt for cnt in contours if cv2.contourArea(cnt) > 1000]
        total_pixels = gray.size
        building_pixels = sum(cv2.contourArea(cnt) for cnt in building_contours)
        building_percentage = (building_pixels / total_pixels) * 100

        print(f"Building Coverage: {building_percentage:.2f}%")

        if building_percentage > 3:
            print("Existing Structures: YES")
            print("Recommendation: Double-check with secondary sources for accuracy.")
        else:
            print("Existing Structures: NO")
    else:
        print("Error fetching the map image.")

elevation1 = get_elevation(*point1)
elevation2 = get_elevation(*point2)

get_trees_data(point1[0], point2[1])
get_building_data(point1[0], point2[1])

if elevation1 is not None and elevation2 is not None:
    rise = abs(elevation2 - elevation1)
    run = 5
    slope = (rise / run) * 100

    print(f"Slope: {slope:.2f}%")
else:
    print("Error fetching elevation data.")