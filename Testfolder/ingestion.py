from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By # For finding elements
from selenium.webdriver.support.ui import WebDriverWait # For waiting
from selenium.webdriver.support import expected_conditions as EC # For expected conditions
import pandas as pd
import time
import random

url = "https://www.wayfair.com/furniture/sb0/sofas-c413892.html?prefetch=true"

# Setup Chrome options for headless Browse and to avoid bot detection
chrome_options = Options()
# chrome_options.add_argument("--headless") # Uncomment this line to run Chrome in the background (no visible browser window)
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("--disable-gpu") # Recommended for headless
chrome_options.add_argument("--window-size=1920,1080") # Set a common screen size
chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36")
chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
chrome_options.add_experimental_option('useAutomationExtension', False)

# Initialize the Chrome WebDriver
# This will automatically download the correct chromedriver if it's not present
driver = None
try:
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)

    print(f"Navigating to: {url}")
    driver.get(url)

    # Add a random initial wait to let the page load completely, including JavaScript content
    # You might need to adjust this based on observation
    time.sleep(random.uniform(5, 10))

    # Scroll down the page to load more products (Wayfair often lazy-loads)
    # You might need to scroll multiple times
    scroll_count = 0
    max_scrolls = 3 # Adjust this based on how many products you want to load
    for _ in range(max_scrolls):
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(random.uniform(2, 4)) # Wait after each scroll
        scroll_count += 1
        print(f"Scrolled down {scroll_count} time(s).")

    # Get the page source after all JavaScript has executed and content has loaded
    page_source = driver.page_source

    # Now, parse the page source with BeautifulSoup
    soup = BeautifulSoup(page_source, 'html.parser')

    # Find all product containers
    # IMPORTANT: The class names might still change or be dynamically generated.
    # You MUST inspect the live page in your browser's developer tools (Cmd+Option+I on Mac)
    # after the page fully loads, to find the most accurate class names for products.
    # Look for a common container div that holds individual product cards.

    # Common Wayfair product card selectors (may need adjustment)
    product_containers = soup.find_all('div', class_=lambda c: c and 'ProductCard' in c) # More flexible match
    if not product_containers:
        # Try another common pattern if the first one doesn't yield results
        product_containers = soup.find_all('div', class_=lambda c: c and 'browse-ProductCard' in c)

    products = []
    if not product_containers:
        print("No product containers found after Selenium load. Check selectors.")
        print(f"Partial HTML for debugging (first 2000 chars): {soup.prettify()[:2000]}")
    else:
        print(f"Found {len(product_containers)} potential product containers.")
        for container in product_containers:
            title_element = container.find('div', class_=lambda c: c and 'ProductCard-title' in c) # Often a div wrapping text
            # Wayfair prices are tricky; often in a span with a data-testid
            price_element = container.find('span', {'data-testid': 'ProductPrice'})
            if not price_element:
                price_element = container.find('div', class_=lambda c: c and 'ProductCard-price' in c) # Fallback

            if title_element and price_element:
                title = title_element.get_text(strip=True)
                price = price_element.get_text(strip=True)
                products.append({
                    'title': title,
                    'price': price
                })
            else:
                # This helps in debugging: if you're not getting all products,
                # you can print the container's HTML to see what's missing.
                # print(f"Skipping product: Title or price element not found in container: {container.prettify()[:500]}...")
                pass

    df = pd.DataFrame(products)

    if not df.empty:
        df.to_csv('wayfair_sofas.csv', index=False)
        print(f"\nData saved to wayfair_sofas.csv. Number of products found: {len(df)}")
        print(df)
    else:
        print("\nNo products extracted. DataFrame is empty.")
        print("This often means your CSS selectors for title/price are incorrect or Wayfair's structure changed.")

except Exception as e:
    print(f"An error occurred: {e}")
finally:
    if driver:
        driver.quit() # Always close the browser when done