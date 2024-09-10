import asyncio
from pyppeteer import launch
import logging
from bs4 import BeautifulSoup
import json

logging.basicConfig(level=logging.INFO)

class DoctorScraper:
    def __init__(self):
        self.max_doctors = 5
        self.browser = None

    async def init_browser(self):
        logging.info("Initializing browser...")
        self.browser = await launch(
            headless=False,
            args=['--no-sandbox', '--disable-setuid-sandbox'],
            defaultViewport=None,
            autoClose=False,
            executablePath='/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',
            userDataDir='./user_data'
        )
        logging.info("Browser initialized successfully")

    async def fetch_page(self, url):
        if not self.browser:
            await self.init_browser()
        page = await self.browser.newPage()
        await page.setViewport({"width": 1280, "height": 800})
        
        try:
            logging.info(f"Navigating to {url}")
            await page.goto(url, {'waitUntil': 'networkidle0', 'timeout': 60000})
            logging.info("Page loaded")

            # Wait for the specialty list to be visible
            await page.waitForSelector('.center-list-container', {'visible': True, 'timeout': 10000})
            logging.info("Specialty list found")

            # Select a specialty (e.g., "Acupuncturists")
            specialty_selector = "a[href='/acupuncture']"
            logging.info(f"Selecting specialty: {specialty_selector}")
            await page.waitForSelector(specialty_selector, {'visible': True, 'timeout': 10000})
            await page.click(specialty_selector)
            await page.waitForNavigation({'waitUntil': 'networkidle0'})
            logging.info("Navigated to specialty page")

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
            logging.error(f"Error fetching page: {e}", exc_info=True)
            content = await page.content()  # Get content even if there's an error
            status = 500
        finally:
            await page.screenshot({'path': 'debug_screenshot.png', 'fullPage': True})
            logging.info("Saved debug screenshot to debug_screenshot.png")
        
        return page, content, status

    async def scrape_profile(self, page, profile_url):
        try:
            await page.goto(profile_url, {'waitUntil': 'networkidle0', 'timeout': 60000})
            logging.info(f"Navigated to profile: {profile_url}")
            
            # Wait for the profile content to load
            try:
                await page.waitForSelector('.provider-info', {'visible': True, 'timeout': 30000})
            except Exception as e:
                logging.warning(f"Timeout waiting for .provider-info: {e}")
                # Continue with the scraping even if this selector is not found
            
            profile_content = await page.content()
            profile_soup = BeautifulSoup(profile_content, 'html.parser')
            
            # Extract phone number
            phone_number = profile_soup.select_one('.phone a')
            phone_number = phone_number.text.strip() if phone_number else ''
            
            # Extract insurance plans
            insurance_plans = []
            insurance_list = profile_soup.select('.insurances-list li')
            for insurance in insurance_list:
                insurance_plans.append(insurance.text.strip())
            
            # Extract education
            education = []
            education_sections = profile_soup.select('.description.loc-vc-mdschwrp')
            for section in education_sections:
                school = section.select_one('.loc-vc-schl')
                year = section.select_one('.loc-vc-schlyr')
                if school and year:
                    education.append({
                        'school': school.text.strip(),
                        'year': year.text.strip()
                    })
            
            return {
                'phone_number': phone_number,
                'insurance_plans': insurance_plans,
                'education': education
            }
        except Exception as e:
            logging.error(f"Error scraping profile: {e}", exc_info=True)
            return {}

    async def parse_content(self, page, content):
        soup = BeautifulSoup(content, 'html.parser')
        doctors = soup.select("div.webmd-card.provider-result-card")
        logging.info(f"Found {len(doctors)} doctor cards")

        if not doctors:
            logging.warning("No doctor cards found. Dumping page content for debugging.")
            with open('debug_page_content.html', 'w', encoding='utf-8') as f:
                f.write(content)
            logging.info("Saved debug page content to debug_page_content.html")

        scraped_doctors = []
        for doctor in doctors[:self.max_doctors]:
            try:
                doctor_item = {}
                full_name = doctor.select_one("h3 a").text.strip()
                doctor_item['full_name'] = full_name.replace("Dr. ", "").replace("DACM, L.Ac.", "").strip()
                doctor_item['title'] = "Dr."
                doctor_item['specialty'] = doctor.select_one("div.specialty").text.strip()
                doctor_item['location'] = doctor.select_one("address.card-address span.citystate").text.strip()
                doctor_item['country'] = 'USA'
                doctor_item['company'] = ''
                phone_button = doctor.select_one("a.cta-phone-button")
                doctor_item['phone_number'] = phone_button.text.strip() if phone_button else ''
                doctor_item['email'] = ''
                doctor_item['company_domain'] = 'vitals.com'

                name_parts = doctor_item['full_name'].split()
                if len(name_parts) > 1:
                    doctor_item['first_name'] = name_parts[0]
                    doctor_item['last_name'] = ' '.join(name_parts[1:])

                # Extract years of experience
                experience = doctor.select_one("li:contains('years of experience')")
                if experience:
                    doctor_item['years_of_experience'] = experience.text.split()[0]

                # Check if "View Profile" button exists
                profile_link = doctor.select_one("a.readmore")
                if profile_link and profile_link.get('href'):
                    profile_url = f"https://www.vitals.com{profile_link['href']}"
                    try:
                        profile_data = await self.scrape_profile(page, profile_url)
                        doctor_item.update(profile_data)
                    except Exception as e:
                        logging.error(f"Error scraping profile: {e}", exc_info=True)
                        # Continue with the data we have even if profile scraping fails

                logging.info(f"Scraped doctor: {doctor_item}")
                scraped_doctors.append(doctor_item)
            except Exception as e:
                logging.error(f"Error parsing doctor card: {e}", exc_info=True)
        
        return scraped_doctors

    async def scrape(self):
        url = 'https://www.vitals.com/doctors'
        page, content, status = await self.fetch_page(url)
        
        if status == 200:
            doctors = await self.parse_content(page, content)
            logging.info(f"Scraped {len(doctors)} doctors")
            
            # Save to file
            with open('scraped_doctors.json', 'w') as f:
                json.dump(doctors, f, indent=2)
            logging.info("Saved scraped data to scraped_doctors.json")
        else:
            logging.error(f"Failed to fetch page: status {status}")

        await self.browser.close()

async def main():
    scraper = DoctorScraper()
    await scraper.scrape()

if __name__ == "__main__":
    asyncio.run(main())