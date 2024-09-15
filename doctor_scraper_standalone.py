import asyncio
from pyppeteer import launch
import logging
from bs4 import BeautifulSoup
import json
from urllib.parse import urljoin, urlparse
import random
import re

logging.basicConfig(level=logging.INFO)

class DoctorScraper:
    def __init__(self):
        self.max_doctors = 2
        self.browser = None
        self.base_url = 'https://www.vitals.com'

    async def init_browser(self):
        logging.info("Initializing browser...")
        self.browser = await launch(
            headless=False,
            args=['--no-sandbox', '--disable-setuid-sandbox'],
            defaultViewport=None,
            autoClose=False,
            executablePath='/Applications/Google Chrome.app/Contents/MacOS/Google Chrome'
        )
        logging.info("Browser initialized successfully")

    async def fetch_page(self, url, page_number=1):
        if not self.browser:
            await self.init_browser()
        page = await self.browser.newPage()
        await page.setViewport({"width": 1280, "height": 800})
        
        try:
            pagination_url = f"{url}?page={page_number}" if page_number > 1 else url
            logging.info(f"Navigating to {pagination_url}")
            await page.goto(pagination_url, {'waitUntil': 'networkidle0', 'timeout': 60000})
            logging.info(f"Page {page_number} loaded")

            # Wait for doctor cards to load
            await page.waitForSelector('.webmd-card.provider-result-card', {'visible': True, 'timeout': 30000})
            logging.info("Doctor cards loaded")

            # Scroll down to load more results
            logging.info("Scrolling page to load more results")
            for _ in range(5):  # Scroll 5 times
                await page.evaluate('window.scrollBy(0, window.innerHeight)')
                await asyncio.sleep(1)

            logging.info("Fetching final content")
            content = await page.content()
            status = 200
        except Exception as e:
            logging.error(f"Error fetching page {page_number}: {e}", exc_info=True)
            content = await page.content()  # Get content even if there's an error
            status = 500
        
        await page.close()
        return content, status

    async def collect_profile_urls(self, content):
        soup = BeautifulSoup(content, 'html.parser')
        doctors = soup.select("div.webmd-card.provider-result-card")
        profile_urls = []

        for doctor in doctors:
            profile_link = doctor.select_one("a.readmore")
            if profile_link and profile_link.get('href'):
                profile_url = urljoin(self.base_url, profile_link['href'])
                profile_urls.append(profile_url)

        return profile_urls

    async def scrape_profile(self, profile_url):
        page = await self.browser.newPage()
        try:
            await page.goto(profile_url, {'waitUntil': 'networkidle0', 'timeout': 60000})
            logging.info(f"Navigated to profile: {profile_url}")
            
            # Simulate reading the page
            await self.simulate_human_reading(page)
            
            profile_content = await page.content()
            profile_soup = BeautifulSoup(profile_content, 'html.parser')
            
            doctor_item = {
                'full_name': '',
                'title': '',  # Change this line from 'Dr.' to an empty string
                'specialty': '',
                'country': 'USA',
                'company': '',
                'email': '',
                'first_name': '',
                'last_name': '',
                'insurance_plans': [],
                'education': [],
                'years_experience': '',
                'languages': [],
                'locations': [],
                'company_website': '',
                'phone': '' 
            }

            # Extract basic info
            name_element = profile_soup.select_one("h1.loc-vs-fname")
            if name_element:
                full_name = name_element.text.strip()
                doctor_item['full_name'] = full_name
                
                # Extract title and name parts
                name_parts = full_name.split()
                if name_parts:
                    if name_parts[0].lower() in ['dr.', 'dr']:
                        doctor_item['title'] = name_parts[0]
                        name_parts = name_parts[1:]
                    elif name_parts[0].lower() in ['mr.', 'mrs.', 'ms.', 'miss']:
                        doctor_item['title'] = name_parts[0]
                        name_parts = name_parts[1:]
                    
                    if name_parts:
                        doctor_item['first_name'] = name_parts[0]
                        doctor_item['last_name'] = ' '.join(name_parts[1:])

            specialty_element = profile_soup.select_one("div.specialty.loc-vs-dspsplty")
            if specialty_element:
                doctor_item['specialty'] = specialty_element.text.strip()

            # Extract insurance plans
            insurance_list = profile_soup.select('.insurances-list li')
            doctor_item['insurance_plans'] = [insurance.text.strip() for insurance in insurance_list]
            
            # Extract education
            education_sections = profile_soup.select('.description.loc-vc-mdschwrp')
            for section in education_sections:
                school = section.select_one('.loc-vc-schl')
                year = section.select_one('.loc-vc-schlyr')
                if school and year:
                    doctor_item['education'].append({
                        'school': school.text.strip(),
                        'year': year.text.strip()
                    })
            
            # Extract Quick Facts information
            quick_facts = profile_soup.select('.quickfacts-card li')
            for fact in quick_facts:
                fact_text = fact.text.strip()
                if 'years of experience' in fact_text:
                    doctor_item['years_experience'] = fact_text.split()[0]
                elif 'speaks' in fact_text:
                    doctor_item['languages'] = fact_text.replace('speaks', '').strip().split(', ')

            # Extract locations
            location_sections = profile_soup.select('.location-map.show-less.limited-locations')
            for section in location_sections:
                location_lines = section.select('.location-line')
                for line in location_lines:
                    location_name = line.select_one('.title.loc-vl-locna h3')
                    address = line.select_one('.address-first-line.top-spacing.grey-darken-text.loc-vl-locad')
                    loc_city = line.select_one('.loc-vl-loccty')
                    loc_state = line.select_one('.loc-vl-locsta')
                    phone = line.select_one('.phone.top-more-spacing a')
                    
                    if location_name and address and loc_city and loc_state:
                        doctor_item['locations'].append({
                            'name': location_name.text.strip(),
                            'address': address.text.strip(),
                            'city': loc_city.text.strip().rstrip(','),
                            'state': loc_state.text.strip(),
                            'phone': phone.text.strip() if phone else ''
                        })

            # Extract website link and company name
            website_link = profile_soup.select_one('a.visit-website-callout.loc-vs-wbstlnk')
            if website_link and website_link.get('href'):
                doctor_item['company_website'] = website_link['href']
                
                # Extract company name from the URL
                parsed_url = urlparse(doctor_item['company_website'])
                domain = parsed_url.netloc
                
                # Remove 'www.' if present and extract the company name
                if domain.startswith('www.'):
                    domain = domain[4:]
                company_parts = domain.split('.')
                if len(company_parts) > 1:
                    company_name = company_parts[0]
                else:
                    company_name = domain
                
                # Clean up the company name
                doctor_item['company'] = company_name.replace('-', ' ').title()
            else:
                doctor_item['company'] = 'Private'

            # Extract phone number
            try:
                phone_element = profile_soup.select_one('a.webmd-button.webmd-button--primary.material.webmd-button--large.is-shadow.is-capitalized')
                if phone_element and phone_element.get('href', '').startswith('tel:'):
                    doctor_item['phone'] = self.clean_phone_number(phone_element.text.strip())

                # If phone number is not found in the button, try to find it in the location section
                if not doctor_item['phone']:
                    location_phone = profile_soup.select_one('.phone.top-more-spacing a')
                    if location_phone:
                        doctor_item['phone'] = self.clean_phone_number(location_phone.text.strip())
            except Exception as e:
                logging.error(f"Error extracting phone number: {e}", exc_info=True)
                doctor_item['phone'] = ''

            # Clean phone numbers in locations
            for location in doctor_item['locations']:
                location['phone'] = self.clean_phone_number(location['phone'])

            return doctor_item
        except Exception as e:
            logging.error(f"Error scraping profile {profile_url}: {e}", exc_info=True)
            return None
        finally:
            await page.close()

    async def scrape(self):
        url = f'{self.base_url}/acupuncture'  # Start directly with the acupuncture specialty
        all_profile_urls = []
        page_number = 1

        while len(all_profile_urls) < self.max_doctors:
            content, status = await self.fetch_page(url, page_number)
            
            if status == 200:
                profile_urls = await self.collect_profile_urls(content)
                all_profile_urls.extend(profile_urls)
                logging.info(f"Collected {len(profile_urls)} profile URLs from page {page_number}")
                
                if len(profile_urls) == 0:
                    logging.info("No more profiles found on this page. Stopping pagination.")
                    break
                
                page_number += 1
            else:
                logging.error(f"Failed to fetch page {page_number}: status {status}")
                break

            if page_number > 250:  # Safety check to avoid infinite loop
                logging.warning("Reached maximum page limit (250). Stopping pagination.")
                break

        # Trim the list to max_doctors
        all_profile_urls = all_profile_urls[:self.max_doctors]
        logging.info(f"Total profile URLs collected: {len(all_profile_urls)}")

        # Scrape individual profiles
        all_doctors = []
        for profile_url in all_profile_urls:
            doctor_data = await self.scrape_profile(profile_url)
            if doctor_data:
                all_doctors.append(doctor_data)
            
            # Add a more human-like delay
            delay = random.uniform(5, 15)  # Random delay between 5 to 15 seconds
            logging.info(f"Waiting for {delay:.2f} seconds before next request")
            await asyncio.sleep(delay)

            # Occasionally take a longer break
            if random.random() < 0.1:  # 10% chance of a longer break
                long_break = random.uniform(10, 20)  # 15 to 30 seconds break
                logging.info(f"Taking a longer break for {long_break:.2f} seconds")
                await asyncio.sleep(long_break)
        
        logging.info(f"Total doctors scraped: {len(all_doctors)}")
        
        # Save to file
        with open('scraped_doctors_test.json', 'w') as f:
            json.dump(all_doctors, f, indent=2)
        logging.info("Saved scraped data to scraped_doctors_test.json")

        await self.browser.close()

    async def simulate_human_reading(self, page):
        # Simulate scrolling and pausing to read
        total_height = await page.evaluate('() => document.body.scrollHeight')
        viewport_height = await page.evaluate('() => window.innerHeight')
        current_position = 0

        # Determine number of scroll actions (between 2 and 4)
        num_scrolls = random.randint(2, 3)

        for _ in range(num_scrolls):
            # Scroll a random amount
            scroll_amount = random.randint(viewport_height // 2, viewport_height)
            await page.evaluate(f'window.scrollBy(0, {scroll_amount})')
            current_position += scroll_amount

            # Pause for a random duration to simulate reading
            read_time = random.uniform(2, 6)
            logging.info(f"Reading content for {read_time:.2f} seconds")
            await asyncio.sleep(read_time)

            if current_position >= total_height:
                break

        # Sometimes go back up
        if random.random() < 0.2:  # 20% chance to scroll back up
            await page.evaluate('window.scrollTo(0, 0)')
            await asyncio.sleep(random.uniform(1, 2))

        # Final pause before leaving the page
        final_pause = random.uniform(2, 3)
        logging.info(f"Final pause on page for {final_pause:.2f} seconds")
        await asyncio.sleep(final_pause)

    def clean_phone_number(self, phone):
        # Remove all non-digit characters
        digits = re.sub(r'\D', '', phone)
        # Format the number as xxx-xxx-xxxx
        if len(digits) == 10:
            return f"{digits[:3]}-{digits[3:6]}-{digits[6:]}"
        return digits  # Return original digits if not 10 digits long

async def main():
    scraper = DoctorScraper()
    await scraper.scrape()

if __name__ == "__main__":
    asyncio.run(main())