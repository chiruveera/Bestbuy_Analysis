from bs4 import BeautifulSoup
import requests
import pandas as pd

url = "https://www.wayfair.com/furniture/sb0/sofas-c413892.html?prefetch=true"

HEADERS = ({'User-Agent' : 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36', 'Accept-Language': 'en-US, en;q=0.5',})
try:
    # Attempt to fetch the webpage
    webpage = requests.get(url, headers=HEADERS)
    webpage.raise_for_status()  # Raise an error for bad responses (4xx or 5xx)
    print (webpage.text[:1000])  # Print the first 1000 characters of the response for debugging
    print("\n-- End of Raw HTML (first 1000 chars)  --\n")
except requests.exceptions.RequestException as e:
    # Handle any request exceptions
    print(f"An error occurred while fetching the webpage: {e}")
    raise
# Parse the HTML content using BeautifulSoup
soup = BeautifulSoup(webpage.content, 'html.parser')