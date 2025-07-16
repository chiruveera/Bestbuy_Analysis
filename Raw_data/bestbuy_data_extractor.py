# bestbuy_data_extractor.py

import os
import requests
import json
import pandas as pd
import time
from dotenv import load_dotenv

# --- 1. Configuration and API Key Loading ---
# Load environment variables from a .env file (e.g., your API key)
load_dotenv()
BESTBUY_API_KEY = os.getenv("BESTBUY_API_KEY")

# Essential check: Ensure the API key is loaded
if not BESTBUY_API_KEY:
    print("Error: BESTBUY_API_KEY not found in .env file.")
    print("Please ensure it's set correctly in your .env file.")
    exit() # Exit the script if the API key is missing

# API Endpoints
PRODUCTS_API_URL = "https://api.bestbuy.com/v1/products"
REVIEWS_API_URL = "https://api.bestbuy.com/v1/reviews"

# Define how many items per page to request from the API.
# Best Buy API typically allows a maximum of 100 per page for both products and reviews.
PAGE_SIZE = 100

# Define a delay between API calls to respect Best Buy's rate limits.
# This helps prevent your IP from being temporarily blocked.
# Adjust this value (increase it) if you encounter '429 Too Many Requests' errors.
API_CALL_DELAY_SECONDS = 0.5

# Define the category IDs for the desired product types.
# These IDs were identified from Best Buy's API documentation/examples.
# The script will now fetch products from ALL of these categories.
TARGET_CATEGORY_IDS = [
    "abcat0502000",      # Laptops
    "abcat0101000",      # Televisions (TVs)
    "abcat0401000",      # Digital Cameras
    "abcat0204000",      # Headphones
    "pcmcat209400050001" # Mobile Phones (Cell Phones)
]

# --- Optional: Set a maximum number of products to fetch for testing ---
# Set this to a higher number (e.g., 5000) for your final extraction,
# or set to None to fetch all available products within the categories.
MAX_PRODUCTS_TO_COLLECT = 1000 # Your project target

# --- 2. Data Storage Lists ---
# These lists will temporarily hold the extracted data (as dictionaries)
# before converting them into Pandas DataFrames and saving to CSV files.
all_products_data = []
all_reviews_data = []

# --- 3. Helper Function for Safe API Requests ---
def make_api_request(url, params):
    """
    Makes an API GET request with built-in error handling and a delay to respect rate limits.
    
    Args:
        url (str): The base URL for the API endpoint (e.g., PRODUCTS_API_URL or REVIEWS_API_URL).
        params (dict): A dictionary of query parameters to send with the request.
    
    Returns:
        dict or None: The parsed JSON response data if the request is successful, otherwise None.
    """
    time.sleep(API_CALL_DELAY_SECONDS) # Introduce a delay before each API call

    try:
        response = requests.get(url, params=params)
        response.raise_for_status() # Raises an HTTPError for bad responses (4xx or 5xx status codes)

        # Attempt to parse the JSON response
        return response.json()
    
    # Catch specific request-related errors for better debugging
    except requests.exceptions.HTTPError as errh:
        print(f"HTTP Error for {url}: {errh}")
        # Print the response body to get specific error messages from the API (e.g., invalid attribute)
        print(f"Response Body: {errh.response.text}")
    except requests.exceptions.ConnectionError as errc:
        print(f"Error Connecting to {url}: {errc}")
    except requests.exceptions.Timeout as errt:
        print(f"Timeout Error for {url}: {errt}")
    except requests.exceptions.RequestException as err:
        print(f"An unexpected request error occurred for {url}: {err}")
    except json.JSONDecodeError:
        print(f"Error: Could not decode JSON response from {url}. The API might have returned non-JSON.")
        # If JSON decoding fails, print the raw response text for inspection
        print(f"Raw Response: {response.text}")
    return None # Return None if any error occurs

# --- 4. Main Data Extraction Logic ---
print("--- Starting Best Buy Data Extraction (Sprint 2) ---")

# --- Step A: Fetch Products from Multiple Categories ---
# This section iterates through pages of product search results across defined categories.
print("\nInitiating Product Data Fetching...")

# Build the category filter part of the query.
# This creates a string like "categoryPath.id in(id1,id2,id3)"
categories_query_part = "categoryPath.id in(" + ",".join(TARGET_CATEGORY_IDS) + ")"

current_product_page = 1
total_product_pages = 1 # Initialize to 1 to ensure the loop runs at least once

# Loop to fetch all available pages of products that match our criteria
while current_product_page <= total_product_pages:
    # Stop if we've reached the maximum desired products for the project
    if MAX_PRODUCTS_TO_COLLECT is not None and len(all_products_data) >= MAX_PRODUCTS_TO_COLLECT:
        print(f"  Reached target of {MAX_PRODUCTS_TO_COLLECT} products. Stopping product fetch.")
        break

    print(f"  Fetching products page {current_product_page}...")

    # Parameters for the product search query
    product_params = {
        'format': 'json',
        'apiKey': BESTBUY_API_KEY,
        # 'show' specifies which fields to retrieve for each product.
        'show': 'sku,name,customerReviewAverage,customerReviewCount,regularPrice,categoryPath.name,url',
        'pageSize': PAGE_SIZE,
        'page': current_product_page
    }
    
    # The full product query path combines the category filter and the review count filter.
    # Note: We removed the 'search=laptop' as the category filter is now broader.
    product_query_path = f"({categories_query_part}&(customerReviewCount>0))"
    
    # Make the API request to the Products endpoint
    products_data_response = make_api_request(f"{PRODUCTS_API_URL}{product_query_path}", product_params)

    # Process the response if it's successful and contains product data
    if products_data_response and 'products' in products_data_response:
        products = products_data_response['products']
        
        # Update total_product_pages based on the API response for correct loop termination
        total_products_found = products_data_response.get('total', 0)
        total_product_pages = products_data_response.get('totalPages', 1)

        # Iterate through the products on the current page and extract relevant information
        for product in products:
            product_info = {
                'sku': product.get('sku'),
                'name': product.get('name'),
                'customer_review_average': product.get('customerReviewAverage'),
                'customer_review_count': product.get('customerReviewCount'),
                'regular_price': product.get('regularPrice'),
                # categoryPath is a list of dictionaries; join their 'name' values into a single string
                'category_path': ', '.join([c.get('name') for c in product.get('categoryPath', []) if c.get('name')]),
                'url': product.get('url')
            }
            all_products_data.append(product_info)
        
        print(f"    Added {len(products)} products from page {current_product_page}. Total products collected: {len(all_products_data)} (out of {total_products_found})")
        current_product_page += 1 # Move to the next page of products
    else:
        # If no products are found on a page or an error occurs, stop fetching products
        print("  No more products or an error occurred during product fetch. Stopping product collection.")
        break # Exit the product fetching loop

print(f"\nFinished fetching products. Total unique products collected: {len(all_products_data)}")


# --- Step B: Fetch Reviews for Each Collected Product ---
# This section iterates through each product we just collected and fetches all its reviews.
print("\nInitiating Review Data Fetching for Collected Products...")
processed_skus = set() # Use a set to keep track of SKUs for which reviews have been fetched (prevents duplicates)

# Loop through each product's information that we stored
for product_info in all_products_data:
    sku = product_info.get('sku')
    
    # Skip if SKU is missing or if we've already processed reviews for this SKU
    if not sku or sku in processed_skus:
        continue

    print(f"  Fetching reviews for SKU: {sku} ({product_info.get('name', 'Unknown Product')[:60]}...)")
    
    current_review_page = 1
    total_review_pages = 1 # Initialize to 1 to ensure the inner loop runs at least once

    # Inner loop to fetch all pages of reviews for the current product (SKU)
    while current_review_page <= total_review_pages:
        review_params = {
            'format': 'json',
            'apiKey': BESTBUY_API_KEY,
            # 'show' specifies which fields to retrieve for each review.
            # Using 'submissionTime' as 'submissionDate' is not a valid attribute.
            'show': 'id,sku,comment,rating,title,reviewer.name,submissionTime', 
            'pageSize': PAGE_SIZE,
            'page': current_review_page
        }
        
        # Reviews API query path for a specific SKU.
        # This targets reviews only for the current product being processed.
        review_query_path = f"(sku={sku})"
        
        # Make the API request to the Reviews endpoint
        reviews_data_response = make_api_request(f"{REVIEWS_API_URL}{review_query_path}", review_params)

        # Process the response if it's successful and contains review data
        if reviews_data_response and 'reviews' in reviews_data_response:
            reviews = reviews_data_response['reviews']
            
            # Update total_review_pages based on the API response for correct inner loop termination
            total_reviews_for_sku = reviews_data_response.get('total', 0)
            total_review_pages = reviews_data_response.get('totalPages', 1)

            # Iterate through reviews on the current page and extract relevant information
            for review in reviews:
                # Safely extract reviewer name, handling cases where 'reviewer' might not be a dictionary or is missing.
                reviewer_info = review.get('reviewer')
                reviewer_name = reviewer_info.get('name', 'N/A') if isinstance(reviewer_info, dict) else 'N/A'

                review_info = {
                    'review_id': review.get('id'),
                    'product_sku': review.get('sku'), # This links the review back to its product
                    'review_title': review.get('title'),
                    'review_comment': review.get('comment'),
                    'review_rating': review.get('rating'),
                    'reviewer_name': reviewer_name,
                    'submission_time': review.get('submissionTime')
                }
                all_reviews_data.append(review_info)
            
            print(f"    Added {len(reviews)} reviews for SKU {sku} (page {current_review_page}). Total reviews collected: {len(all_reviews_data)}")
            current_review_page += 1 # Move to the next review page for this product
        else:
            # If no more reviews are found for the current SKU or an error occurs, stop fetching its reviews
            print(f"  No more reviews or an error occurred fetching reviews for SKU {sku}. Stopping review collection for this product.")
            break # Exit the inner review fetching loop
    
    processed_skus.add(sku) # Mark this SKU as having its reviews processed

print(f"\nFinished fetching reviews. Total reviews collected: {len(all_reviews_data)}")

# --- 5. Convert to Pandas DataFrames and Save to CSV Files ---
print("\nConverting collected data to Pandas DataFrames and saving to CSV files...")

# Create DataFrame for Products
if all_products_data:
    products_df = pd.DataFrame(all_products_data)
    # Ensure SKU is a string type to prevent potential data type issues during merging/joining later
    products_df['sku'] = products_df['sku'].astype(str)
    products_df.to_csv('bestbuy_products_raw.csv', index=False, encoding='utf-8')
    print(f"Successfully saved {len(products_df)} products to 'bestbuy_products_raw.csv'")
else:
    print("No product data collected to save to 'bestbuy_products_raw.csv'.")

# Create DataFrame for Reviews
if all_reviews_data:
    reviews_df = pd.DataFrame(all_reviews_data)
    # Ensure product_sku is a string type for consistent merging/joining with products_df
    reviews_df['product_sku'] = reviews_df['product_sku'].astype(str)
    reviews_df.to_csv('bestbuy_reviews_raw.csv', index=False, encoding='utf-8')
    print(f"Successfully saved {len(reviews_df)} reviews to 'bestbuy_reviews_raw.csv'")
else:
    print("No review data collected to save to 'bestbuy_reviews_raw.csv'.")

print("\n--- Data Extraction (Sprint 2) Complete! ---")
print("You now have two raw data files:")
print("- 'bestbuy_products_raw.csv' (containing product details from multiple categories)")
print("- 'bestbuy_reviews_raw.csv' (containing all customer reviews, linked by 'product_sku')")
print("\nYour next step will be to work on Sprint 3: Data Cleaning & Structuring.")