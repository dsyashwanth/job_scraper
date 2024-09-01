import json
from datetime import datetime
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import time
import logging
from dotenv import load_dotenv
import os

# logging Setup 
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()

load_dotenv()

urls = [
    "https://www.linkedin.com/jobs/search?location=India&geoId=102713980&f_C=1035&position=1&pageNum=0",
    "https://www.linkedin.com/jobs/search?keywords=&location=India&geoId=102713980&f_C=1441&position=1&pageNum=0",
    "https://www.linkedin.com/jobs/search?keywords=&location=India&geoId=102713980&f_TPR=r86400&f_C=1586&position=1&pageNum=0"
]

# Chrome WebDriver Setup 
chrome_options = Options()
chrome_options.add_argument("--headless")

chromedriver_path = os.getenv('CHROMEDRIVER_PATH')

job_list = []

def create_driver():
    service = Service(chromedriver_path)
    driver = webdriver.Chrome(service=service, options=chrome_options)
    driver.maximize_window()
    return driver

def extract_job_details(driver, soup):
    job_listings = soup.find('ul', {'class': 'jobs-search__results-list'}).find_all('li')
    if not job_listings:
        logger.warning("No job listings found.")
    
    job_counter = 0
    for listing in job_listings:
        if job_counter >= 25:
            break
        try:
            job_data = {
                'Job title': listing.find('h3', {'class': 'base-search-card__title'}).text.strip() if listing.find('h3', {'class': 'base-search-card__title'}) else None,
                'Company': listing.find('h4', {'class': 'base-search-card__subtitle'}).find('a').text.strip() if listing.find('h4', {'class': 'base-search-card__subtitle'}) and listing.find('h4', {'class': 'base-search-card__subtitle'}).find('a') else None,
                'Location': listing.find('span', {'class': 'job-search-card__location'}).text.strip() if listing.find('span', {'class': 'job-search-card__location'}) else None,
                'Link': listing.find('a')['href'] if listing.find('a') else None
            }

            posted_element = listing.find('time', {'class': 'job-search-card__listdate'}) or listing.find('time', {'class': 'job-search-card__listdate--new'})
            if posted_element:
                job_data['Posted on'] = posted_element.text.strip()
                job_data['Posted date'] = datetime.strptime(posted_element['datetime'], "%Y-%m-%d").strftime("%d-%m-%Y")
            else:
                job_data['Posted on'] = None
                job_data['Posted date'] = None

            job_link = listing.find('a', {'class': 'base-card__full-link'})['href']
            if job_link:
                driver.get(job_link)
                time.sleep(5)
                job_soup = BeautifulSoup(driver.page_source, 'html.parser')
                job_criteria = job_soup.find('ul', {'class': 'description__job-criteria-list'})
                if job_criteria:
                    for item in job_criteria.find_all('li'):
                        header = item.find('h3', {'class': 'description__job-criteria-subheader'})
                        if header:
                            key = header.text.strip()
                            value = item.find('span', {'class': 'description__job-criteria-text'}).text.strip()
                            job_data[key] = value

            job_list.append(job_data)
            job_counter += 1

        except Exception as e:
            logger.error(f"Error extracting job details: {e}")

def save_data():
    try:
        timestamp = datetime.now().strftime('%d%m%Y_%H%M%S')
        json_folder = 'json'
        csv_folder = 'csv'
        os.makedirs(json_folder, exist_ok=True)
        os.makedirs(csv_folder, exist_ok=True)

        json_filename = os.path.join(json_folder, f'{timestamp}_jobs.json')
        csv_filename = os.path.join(csv_folder, f'{timestamp}_jobs.csv')

        with open(json_filename, 'w') as f:
            json.dump(job_list, f, indent=4)
        logger.info(f"Data saved to {json_filename}")

        df = pd.DataFrame(job_list)
        df.to_csv(csv_filename, index=False)
        logger.info(f"Data saved to {csv_filename}")
    except Exception as e:
        logger.error(f"Error saving data: {e}")

for url in urls:
    retries = 5
    while retries > 0:
        driver = create_driver()
        try:
            logger.info(f"Scraping URL: {url}")
            driver.get(url)
            time.sleep(5)

            if driver.current_url != url:
                logger.error("Login required. Retry with new driver.")
                driver.quit()
                retries -= 1
                continue

            WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.CLASS_NAME, 'base-serp-page__content')))
            time.sleep(5)  
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            extract_job_details(driver, soup)
            print(f"Total jobs scraped: {len(job_list)}")
            driver.quit()  # Close driver after processing each URL
            break  # Exit the retry loop if successful
        except Exception as e:
            logger.error(f"Error scraping URL {url}: {e}")
            driver.quit()
            retries -= 1
            time.sleep(5)  
            if retries == 0:
                logger.error(f"Failed to scrape URL {url} after multiple attempts")

if job_list:
    save_data()

logger.info("Scraping complete.")