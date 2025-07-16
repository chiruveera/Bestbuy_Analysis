import os
import requests
from dotenv import load_dotenv

# Load API key from .env file
load_dotenv()
BESTBUY_API_KEY = os.getenv("BESTBUY_API_KEY")

if not BESTBUY_API_KEY:
    print(" API key not found in .env file.")
    exit()

# API Endpoints
PRODUCTS_API_URL = "https://api.bestbuy.com/v1/products"
REVIEWS_API_URL = "https://api.bestbuy.com/v1/reviews"

# Parameters
product_params = {
    'format': 'json',
    'apiKey': BESTBUY_API_KEY,
    'show': 'sku,name,customerReviewAverage,customerReviewCount,regularPrice,categoryPath.name,url',
    'pageSize': 2
}

review_params = {
    'format': 'json',
    'apiKey': BESTBUY_API_KEY,
    'show': 'id,sku,title,comment,rating,reviewer.name,submissionTime',
    'pageSize': 2  # Limit to 3 reviews per product
}

# Categories you want to pull
category_ids = {
    'Laptops': 'abcat0502000',
    'Mobile Phones': 'abcat0801000',
    'TVs': 'abcat0101000',
    'Cameras': 'abcat0401000'
}

# === Fetch Products and Reviews ===
for category, cat_id in category_ids.items():
    print(f"\n🔍 Category: {category}\n" + "-"*60)

    try:
        response = requests.get(
            f"{PRODUCTS_API_URL}((categoryPath.id={cat_id})&(customerReviewCount>0))",
            params=product_params
        )
        response.raise_for_status()
        products = response.json().get('products', [])

        if not products:
            print("No products found.")
            continue

        for i, product in enumerate(products):
            print(f"\n🛒 Product {i + 1}: {product.get('name')}")
            print(f"   ➤ SKU: {product.get('sku')}")
            print(f"   ➤ Avg Rating: {product.get('customerReviewAverage')} ⭐️")
            print(f"   ➤ Review Count: {product.get('customerReviewCount')}")
            print(f"   ➤ Price: ${product.get('regularPrice')}")
            print(f"   ➤ URL: {product.get('url')}")
            print("   ➤ Categories:", ", ".join(
                [c.get('name') for c in product.get('categoryPath', [])]))

            # --- Fetch Reviews for this SKU ---
            sku = str(product.get('sku'))
            review_response = requests.get(
                f"{REVIEWS_API_URL}(sku={sku})",
                params=review_params
            )
            review_response.raise_for_status()
            reviews = review_response.json().get('reviews', [])

            if not reviews:
               print("   💬 No reviews found.")
            else:
                print("   💬 Top Reviews:")
                for r, review in enumerate(reviews):
                    print(f"     • {review.get('title')} ({review.get('rating')}⭐️)")
                    print(f"       {review.get('comment')}")
                    reviewer_info = review.get('reviewer')
                    if isinstance(reviewer_info, list):
                        reviewer_name = reviewer_info[0].get('name', 'Anonymous') if reviewer_info else 'Anonymous'
                    elif isinstance(reviewer_info, dict):
                        reviewer_name = reviewer_info.get('name', 'Anonymous')
                else:
                    reviewer_name = 'Anonymous'

                    print(f"       — {reviewer_name}, {review.get('submissionTime')}")
                    print()
    except requests.exceptions.RequestException as e:
        print(f"Error: {e}")
