import requests
from bs4 import BeautifulSoup
import csv
import boto3
from io import StringIO
import datetime
import os
from dotenv import load_dotenv

# Load environment variables from .env file
print("Current working directory:", os.getcwd())
load_dotenv()

# Debug prints to verify environment variables
access_key = os.getenv('accesss-key-id')
secret_key = os.getenv('secret-access-key')
print("Access key loaded:", bool(access_key))
print("Secret key loaded:", bool(secret_key))

# AWS S3 Details
S3_BUCKET = "mansi-etl-bucket"
S3_CLIENT = boto3.client(
    "s3",
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key
)


# Scrape Books from BooksToScrape
def scrape_books():
    base_url = "http://books.toscrape.com/catalogue/category/books_1/"
    books = []

    # Iterate through 10 pages
    for page in range(1, 16):
        # Construct page URL
        if page == 1:
            url = f"{base_url}index.html"
        else:
            url = f"{base_url}page-{page}.html"

        print(f"Scraping page {page}...")
        response = requests.get(url)
        soup = BeautifulSoup(response.content, "html.parser")

        # Extract books from current page
        for book in soup.find_all("article", class_="product_pod"):
            title = book.h3.a.attrs["title"]
            price = book.find("p", class_="price_color").text
            stock = book.find("p", class_="instock availability").text.strip()
            books.append([title, price, stock])

    return books

def save_locally(data, filename):
    # Create 'data' directory if it doesn't exist
    os.makedirs('data', exist_ok=True)
    
    # Full path for the CSV file
    filepath = os.path.join('data', filename)
    
    # Save to local CSV file
    with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["title", "price", "stock"])
        writer.writerows(data)
    print(f"Saved data locally to: {filepath}")

# Store Data in S3 as CSV
def save_to_s3(data):
    current_time = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    csv_file_name = f"raw/book_data_{current_time}.csv"

    # Convert Data to CSV Format
    csv_buffer = StringIO()
    writer = csv.writer(csv_buffer)
    writer.writerow(["title", "price", "stock"])
    writer.writerows(data)

    # Upload CSV to S3
    S3_CLIENT.put_object(
        Bucket=S3_BUCKET,
        Key=csv_file_name,
        Body=csv_buffer.getvalue()
    )
    print(f"Uploaded {csv_file_name} to S3")

if __name__ == "__main__":
    books_data = scrape_books()
    print(f"Total books scraped: {len(books_data)}")
        # Generate filename with timestamp
    timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    filename = f"book_data_{timestamp}.csv"
    save_locally(books_data, filename)
    save_to_s3(books_data)
