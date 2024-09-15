import asyncio
from pyppeteer import launch
import logging
from bs4 import BeautifulSoup
import json
from urllib.parse import urljoin, urlparse
import random
import re
import aiohttp
import ssl
import certifi
from datetime import datetime
from fuzzywuzzy import fuzz

logging.basicConfig(level=logging.INFO)

class DoctorScraper:
    def __init__(self):
        self.max_doctors = 2
        self.browser = None
        self.base_url = 'https://www.vitals.com'
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36 Edg/91.0.864.59'
        ]
        self.current_user_agent = self.get_random_user_agent()
        self.request_count = 0

    def get_random_user_agent(self):
        return random.choice(self.user_agents)

    def get_current_user_agent(self):
        self.request_count += 1
        if self.request_count % 20 == 0:
            self.current_user_agent = self.get_random_user_agent()
        return self.current_user_agent

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
        
        # Use the current user agent
        await page.setUserAgent(self.get_current_user_agent())
        
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
            await page.setUserAgent(self.get_current_user_agent())
            await page.goto(profile_url, {'waitUntil': 'networkidle0', 'timeout': 60000})
            logging.info(f"Navigated to profile: {profile_url}")
            
            await self.simulate_human_reading(page)
            
            profile_content = await page.content()
            profile_soup = BeautifulSoup(profile_content, 'html.parser')
            
            doctor_item = {
                'full_name': '',
                'title': '',
                'specialties': [],
                'country': 'USA',
                'company': '',
                'email_history': [],
                'first_name': '',
                'last_name': '',
                'insurance_plans': [],
                'education': [],
                'years_experience': 'N/A',
                'languages': [],
                'locations': [],
                'company_website': 'N/A',
                'phone': '',
                'npi': '',
                'gender': 'N/A',
                'email_confidence': {}
            }

            # Extract basic info
            name_element = profile_soup.select_one("h1.loc-vs-fname")
            if name_element:
                full_name = name_element.text.strip()
                doctor_item['full_name'] = full_name
                
                # Extract title
                title_match = re.match(r'^(Dr\.|Mr\.|Mrs\.|Ms\.|Miss)\s', full_name)
                if title_match:
                    doctor_item['title'] = title_match.group(1)
                    full_name = full_name[len(doctor_item['title']):].strip()
                
                # Use extract_name method to separate first and last name
                first_name, last_name = self.extract_name(full_name)
                doctor_item['first_name'] = first_name
                doctor_item['last_name'] = last_name

            specialty_elements = profile_soup.select("div.specialty.loc-vs-dspsplty")
            individual_specialty_elements = profile_soup.select("div.loc-vs-dspsplty")
            
            specialties = []
            for specialty in specialty_elements + individual_specialty_elements:
                # Split the specialties if they're comma-separated
                split_specialties = [s.strip() for s in specialty.text.split(',')]
                specialties.extend(split_specialties)
            
            # Remove duplicates and empty strings
            doctor_item['specialties'] = list(set(s for s in specialties if s))

            # Extract insurance plans
            insurance_list = profile_soup.select('.insurances-list li')
            raw_insurance_plans = [insurance.text.strip() for insurance in insurance_list]
            
            # Clean and standardize insurance plans
            doctor_item['insurance_plans'] = self.clean_insurance_plans(raw_insurance_plans)
            
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
                            'phone': self.clean_phone_number(phone.text.strip()) if phone else ''
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
                doctor_item['company_website'] = 'N/A'

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

            # Extract company email
            if doctor_item['company_website'] != 'N/A':
                company_email = await self.extract_company_email(doctor_item['company_website'])
                if company_email:
                    doctor_item['email_history'].append({
                        "email": company_email,
                        "type": "company",
                        "source": "Company Website",
                        "updated_at": self.get_current_timestamp()
                    })
                    doctor_item['email_confidence'][company_email] = 0.9

            # Extract NPI number
            profile_header = profile_soup.select_one('.profile-header-container')
            if profile_header and 'data-qa-npi' in profile_header.attrs:
                doctor_item['npi'] = profile_header['data-qa-npi']
            else:
                doctor_item['npi'] = 'N/A'

            # Fetch work email and gender from NPI registry
            if doctor_item['npi'] != 'N/A':
                npi_data = await self.fetch_work_email_from_npi(doctor_item['npi'])
                if npi_data['work_email'] != 'N/A':
                    doctor_item['email_history'].append({
                        "email": npi_data['work_email'],
                        "type": "work",
                        "source": "Public Registry",
                        "updated_at": npi_data['last_updated']
                    })
                    doctor_item['email_confidence'][npi_data['work_email']] = 0.6
                doctor_item['gender'] = npi_data['gender']

            return doctor_item
        except Exception as e:
            logging.error(f"Error scraping profile {profile_url}: {e}", exc_info=True)
            return None
        finally:
            await page.close()

    async def fetch_work_email_from_npi(self, npi):
        url = f"https://npiregistry.cms.hhs.gov/api/?version=2.1&number={npi}&limit=100"
        ssl_context = ssl.create_default_context(cafile=certifi.where())
        async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=ssl_context)) as session:
            try:
                async with session.get(url) as response:
                    if response.status == 200:
                        data = await response.json()
                        if data['result_count'] > 0:
                            results = data['results'][0]
                            npi_data = {
                                'work_email': 'N/A',
                                'gender': 'N/A',
                                'last_updated': 'N/A'
                            }
                            
                            # Extract work email
                            endpoints = results.get('endpoints', [])
                            for endpoint in endpoints:
                                if endpoint.get('endpointType') == 'DIRECT':
                                    npi_data['work_email'] = endpoint.get('endpoint', 'N/A')
                                    break
                            
                            # Extract gender
                            basic_info = results.get('basic', {})
                            npi_data['gender'] = basic_info.get('gender', 'N/A')
                            
                            # Extract last updated timestamp
                            last_updated_epoch = results.get('last_updated_epoch', 'N/A')
                            if last_updated_epoch != 'N/A':
                                last_updated_datetime = datetime.utcfromtimestamp(int(last_updated_epoch) / 1000)
                                npi_data['last_updated'] = last_updated_datetime.isoformat()
                            
                            return npi_data
            except Exception as e:
                logging.error(f"Error fetching NPI data: {e}")
        return {'work_email': 'N/A', 'gender': 'N/A', 'last_updated': 'N/A'}

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

    def extract_name(self, full_name):
        # Remove known titles and qualifications
        clean_name = re.sub(r'\b(Dr\.?|Mr\.?|Mrs\.?|Ms\.?|Miss|DACM|L\.Ac\.?|MD|DO|PhD|RN|NP|PA(-C)?|DC)\b', '', full_name).strip()
        clean_name = re.sub(r'[,.]', '', clean_name).strip()  # Remove commas and periods
        
        name_parts = clean_name.split()
        
        if len(name_parts) == 2:
            return name_parts[0], name_parts[1]
        elif len(name_parts) > 2:
            first_name = ' '.join(name_parts[:-1])
            last_name = name_parts[-1]
            return first_name, last_name
        else:
            return clean_name, ''

    async def extract_company_email(self, website_url):
        page = await self.browser.newPage()
        await page.setUserAgent(self.get_current_user_agent())

        try:
            await page.goto(website_url, {'waitUntil': 'networkidle0', 'timeout': 60000})
            logging.info(f"Navigated to company website: {website_url}")

            # Look for common contact page links
            contact_links = await page.evaluate('''
                () => {
                    const links = Array.from(document.querySelectorAll('a'));
                    return links
                        .filter(link => link.textContent.toLowerCase().includes('contact'))
                        .map(link => link.href);
                }
            ''')

            # If contact page found, navigate to it
            if contact_links:
                contact_url = urljoin(website_url, contact_links[0])
                await page.goto(contact_url, {'waitUntil': 'networkidle0', 'timeout': 60000})
                logging.info(f"Navigated to contact page: {contact_url}")

            # Extract email from the page
            email = await page.evaluate('''
                () => {
                    const emailRegex = /[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}/;
                    const pageText = document.body.innerText;
                    const match = pageText.match(emailRegex);
                    return match ? match[0] : null;
                }
            ''')

            if email:
                logging.info(f"Found company email: {email}")
                return email
            else:
                logging.info("No email found on the company website")
                return ''

        except Exception as e:
            logging.error(f"Error extracting company email: {e}", exc_info=True)
            return ''
        finally:
            await page.close()

    def get_current_timestamp(self):
        return datetime.now().isoformat()

    def clean_insurance_plans(self, plans):
        # Convert to list and remove exact duplicates
        unique_plans = list(set(plans))
        
        # Sort plans by length (longer names first) to prioritize more specific names
        unique_plans.sort(key=len, reverse=True)
        
        standardized_plans = []
        while unique_plans:
            current_plan = unique_plans.pop(0)
            similar_plans = [current_plan]
            
            # Compare with remaining plans
            i = 0
            while i < len(unique_plans):
                if fuzz.ratio(current_plan.lower(), unique_plans[i].lower()) >= 90:
                    similar_plans.append(unique_plans.pop(i))
                else:
                    i += 1
            
            # Choose the most common plan name from the similar plans
            most_common = max(set(similar_plans), key=similar_plans.count)
            standardized_plans.append(most_common)
        
        return sorted(standardized_plans)

async def main():
    scraper = DoctorScraper()
    await scraper.scrape()

if __name__ == "__main__":
    asyncio.run(main())