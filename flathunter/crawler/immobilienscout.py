"""Expose crawler for ImmobilienScout"""
import time
from typing import Optional
import datetime
import re

from bs4 import BeautifulSoup, Tag
from jsonpath_ng.ext import parse
import pyotp
from selenium.common.exceptions import JavascriptException, TimeoutException
from selenium.webdriver import Chrome
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC

from flathunter.abstract_crawler import Crawler
from flathunter.logging import logger
from flathunter.chrome_wrapper import get_chrome_driver
from flathunter.exceptions import DriverLoadException

STATIC_URL_PATTERN = re.compile(r'https://www\.immobilienscout24\.de')

def get_result_count(soup: BeautifulSoup) -> int:
    """Scrape the result count from the returned page"""
    def is_result_count_element(element) -> bool:
        if not isinstance(element, Tag):
            return False
        if not element.has_attr('data-is24-qa'):
            return False
        return element.attrs['data-is24-qa'] == 'resultlist-resultCount'

    count_element = soup.find(is_result_count_element)
    if not isinstance(count_element, Tag):
        return 0
    return int(count_element.text.replace('.', ''))

class Immobilienscout(Crawler):
    """Implementation of Crawler interface for ImmobilienScout"""

    URL_PATTERN = STATIC_URL_PATTERN

    JSON_PATH_PARSER_ENTRIES = parse("$..['resultlist.realEstate']")
    JSON_PATH_PARSER_IMAGES = parse("$..galleryAttachments"
                                    "..attachment[?'@xsi.type'=='common:Picture']"
                                    "..['@href'].`sub(/(.*\\\\.jpe?g).*/, \\\\1)`")

    RESULT_LIMIT = 50

    FALLBACK_IMAGE_URL = "https://www.static-immobilienscout24.de/statpic/placeholder_house/" + \
                         "496c95154de31a357afa978cdb7f15f0_placeholder_medium.png"

    def __init__(self, config):
        super().__init__(config)

        self.config = config
        self.auth = config.get_auth("immoscout")
        self.driver = None
        self.checkbox = False
        self.afterlogin_string = None
        if "immoscout_cookie" in self.config:
            self.set_cookie()
        if config.captcha_enabled():
            self.checkbox = config.get_captcha_checkbox()
            self.afterlogin_string = config.get_captcha_afterlogin_string()

    def get_driver(self) -> Optional[Chrome]:
        """Lazy method to fetch the driver as required at runtime"""
        if self.driver is not None:
            return self.driver
        if not (self.config.captcha_enabled() and self.captcha_solver or self.config.get_is_captcha_manual()):
            return None
        driver_arguments = self.config.captcha_driver_arguments()
        self.driver = get_chrome_driver(driver_arguments, not self.config.get_is_captcha_manual())
        return self.driver

    def get_driver_force(self) -> Chrome:
        """Fetch the driver, and throw an exception if it is not configured or available"""
        res = self.get_driver()
        if res is None:
            raise DriverLoadException("Unable to load chrome driver when expected")
        return res

    def get_results(self, search_url, max_pages=None):
        """Loads the exposes from the ImmoScout site, starting at the provided URL"""
        # convert to paged URL
        # if '/P-' in search_url:
        #     search_url = re.sub(r"/Suche/(.+?)/P-\d+", "/Suche/\1/P-{0}", search_url)
        # else:
        #     search_url = re.sub(r"/Suche/(.+?)/", r"/Suche/\1/P-{0}/", search_url)
        if '&pagenumber' in search_url:
            search_url = re.sub(r"&pagenumber=[0-9]", "&pagenumber={0}", search_url)
        else:
            search_url = search_url + '&pagenumber={0}'
        logger.debug("Got search URL %s", search_url)

        # load first page to get number of entries
        page_no = 1
        soup = self.get_page(search_url, self.get_driver(), page_no)

        # If we are using Selenium, just parse the results from the JSON in the page response
        if self.get_driver() is not None:
            return self.get_entries_from_javascript()

        no_of_results = get_result_count(soup)

        # get data from first page
        entries = self.extract_data(soup)

        # iterate over all remaining pages
        while len(entries) < min(no_of_results, self.RESULT_LIMIT) and \
                (max_pages is None or page_no < max_pages):
            logger.debug(
                '(Next page) Number of entries: %d / Number of results: %d',
                len(entries), no_of_results)
            page_no += 1
            soup = self.get_page(search_url, self.get_driver(), page_no)
            cur_entry = self.extract_data(soup)
            if isinstance(cur_entry, list):
                break
            entries.extend(cur_entry)
        return entries

    def get_entries_from_javascript(self):
        """Get entries from JavaScript"""
        try:
            result_json = self.get_driver_force().execute_script('return window.IS24.resultList;')
        except JavascriptException:
            logger.warning("Unable to find IS24 variable in window")
            if "Warum haben wir deine Anfrage blockiert?" in self.get_driver_force().page_source:
                logger.error(
                    "IS24 bot detection has identified our script as a bot - we've been blocked"
                )
            return []
        return self.get_entries_from_json(result_json)

    def get_entries_from_json(self, json):
        """Get entries from JSON"""
        entries = [
            self.extract_entry_from_javascript(entry.value)
                for entry in self.JSON_PATH_PARSER_ENTRIES.find(json)
        ]
        logger.debug('Number of found entries: %d', len(entries))
        return entries

    def extract_entry_from_javascript(self, entry):
        """Get single entry from JavaScript"""

        # the url that is being returned to the frontend has a placeholder for screen size.
        # i.e. (%WIDTH% and %HEIGHT%)
        # The website's frontend fills these variables based on the user's screen size.
        # If we remove this part, the API will return the original size of the image.
        #
        # Before:
        # https://pictures.immobilienscout24.de/listings/$$IMAGE_ID$$.jpg/ORIG/legacy_thumbnail/%WIDTH%x%HEIGHT%3E/format/webp/quality/50
        #
        # After: https://pictures.immobilienscout24.de/listings/$$IMAGE_ID$$.jpg

        images = [image.value for image in self.JSON_PATH_PARSER_IMAGES.find(entry)]

        object_id: int = int(entry.get("@id", 0))
        return {
            'id': object_id,
            'url': f"https://www.immobilienscout24.de/expose/{str(object_id)}",
            'image': images[0] if len(images) else self.FALLBACK_IMAGE_URL,
            'images': images,
            'title': entry.get("title", ''),
            'address': entry.get("address", {}).get("description", {}).get("text", ''),
            'crawler': self.get_name(),
            'price': str(entry.get("price", {}).get("value", '')),
            'total_price':
                str(entry.get('calculatedTotalRent', {}).get("totalRent", {}).get('value', '')),
            'size': str(entry.get("livingSpace", '')),
            'rooms': str(entry.get("numberOfRooms", '')),
            'contact_details': entry.get("contactDetails", {}),
        }

    def set_cookie(self):
        """Sets request header cookie parameter to identify as a logged in user"""
        self.HEADERS['Cookie'] = f'reese84:${self.config["immoscout_cookie"]}'

    def get_page(self, search_url, driver=None, page_no=None):
        """Applies a page number to a formatted search URL and fetches the exposes at that page"""
        return self.get_soup_from_url(
            search_url.format(page_no),
            driver=driver,
            checkbox=self.checkbox,
            afterlogin_string=self.afterlogin_string,
        )

    def get_expose_details(self, expose):
        """Loads additional details for an expose by processing the expose detail URL"""
        soup = self.get_soup_from_url(expose['url'])
        date = soup.find('dd', {"class": "is24qa-bezugsfrei-ab"})
        expose['from'] = datetime.datetime.now().strftime("%2d.%2m.%Y")
        if date is not None:
            if not re.match(r'.*sofort.*', date.text):
                expose['from'] = date.text.strip()
        return expose
    

    def solve_captcha(self, driver):
        # check for captcha
        if re.search("initGeetest", driver.page_source):
            self.resolve_geetest(driver)
        elif re.search("g-recaptcha", driver.page_source):
            self.resolve_recaptcha(driver, False, "")
        


    def login(self, driver):
        """Login to ImmobilienScout"""

        login_button = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CSS_SELECTOR, 'div[title=Anmelden]')))
        logged_in = not login_button.is_displayed()
        if logged_in:
            logger.debug("Already logged in")
            return

        driver.get('https://www.immobilienscout24.de/anmelden')
        self.solve_captcha(driver)

        login_form = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, 'loginForm')))
        if not login_form:
            logger.error("Login form not found, probably already logged in")
            return
        
        login_form.find_element(By.NAME, 'username').send_keys(self.auth["username"])
        login_form.submit()

        self.solve_captcha(driver)
        
        login_form = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, 'loginForm')))
        password = login_form.find_element(By.NAME, 'password')
        password.send_keys(self.auth["password"])

        
        login_form.submit()

        self.solve_captcha(driver)

        otp = WebDriverWait(driver, 30).until(EC.presence_of_element_located((By.CSS_SELECTOR, 'input[name=answer]')))
        if not otp:
            logger.error("OTP input not found")
            return
        
        totp = pyotp.TOTP(self.auth["otp_secret"])    
        otp.send_keys(totp.now())

        otp.submit()

        my_area_visible = WebDriverWait(driver, 10).until(EC.visibility_of_element_located((By.CSS_SELECTOR, 'div[title="Mein Bereich"]')))

    
    def send_email(self, expose):
        """Sends email to contact"""
        logger.debug("Sending email to contact")

        driver = self.get_driver_force()

        self.login(driver)

        driver.get(expose['url'])

        # check for captcha
        if re.search("initGeetest", driver.page_source):
            self.resolve_geetest(driver)
        elif re.search("g-recaptcha", driver.page_source):
            self.resolve_recaptcha(driver, False, "")

        # just accept all cookies
        try: 
            cookie_banner = WebDriverWait(driver, 1).until(EC.presence_of_element_located((By.CSS_SELECTOR, '#usercentrics-root')))
            time.sleep(1)
            accept_all = cookie_banner.shadow_root.find_element(By.CSS_SELECTOR, 'button[data-testid="uc-accept-all-button"]')
            accept_all.click()
        except TimeoutException:
            logger.debug("No cookie banner found")


        try: 
            send_button = driver.find_element(By.CSS_SELECTOR, 'a[data-qa=sendButton]')
            if not send_button:
                logger.error("Send button not found")
                return expose
            send_button.click()
            
            
            form = WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.CSS_SELECTOR, 'form[name="contactFormContainer.form"]')))
            if not form:
                logger.error("Form not found")
                return expose
            
            time.sleep(1)
            message = WebDriverWait(driver, 3).until(EC.visibility_of_element_located((By.NAME, 'message')))
            message.clear()

            salutation = "Sehr geehrte Damen und Herren"
            gender = expose.get('contact_details', {}).get('salutation', None)
            lastname = expose.get('contact_details', {}).get('lastname', None)
            if lastname:
                if gender == 'FEMALE':
                    salutation = f"Sehr geehrte Frau {lastname}"
                elif gender == 'MALE':
                    salutation = f"Sehr geehrter Herr {lastname}"

            message.send_keys(self.config.get_auto_email_message().format(
                    salutation=salutation,
                    crawler=expose.get('crawler', 'N/A'),
                    title=expose.get('title', 'N/A'),
                    rooms=expose.get('rooms', 'N/A'),
                    size=expose.get('size', 'N/A'),
                    price=expose.get('price', 'N/A'),
                    url=expose.get('url', 'N/A'),
                    address=expose.get('address', 'N/A'),
                    durations=expose.get('durations', 'N/A')
                ).strip()
            )

            form_elements = form.find_elements(By.CSS_SELECTOR, 'input,select')
            fields = self.config.get_auto_email_fields()
            for element in form_elements:
                name = element.get_attribute('name')
                if element.is_displayed():
                    logger.debug(element.get_attribute('name'))
                    field = fields.get(name, None)
                    if field:
                        element.clear()
                        element.send_keys(field)
                        logger.debug(f"Sending value to {name}: {field}")
            
            time.sleep(1)
            form.find_element(By.CSS_SELECTOR, 'button[data-qa="sendButtonBasic"]').click()
            logger.info("Email sent")
            time.sleep(3)
        except Exception as e:
            logger.exception(e)
            logger.error("Timeout while sending email")
            return expose
        return expose

    # pylint: disable=too-many-locals
    # pylint: disable=too-many-branches
    def extract_data(self, soup):
        """Extracts all exposes from a provided Soup object"""
        entries = []

        results_list = soup.find(id="resultListItems")
        title_elements = results_list.find_all(
            lambda e: e.name == 'a' and e.has_attr('class') and \
                      'result-list-entry__brand-title-container' in e['class']
        ) if results_list else []
        expose_ids = []
        expose_urls = []
        expose_id = 0
        for link in title_elements:
            expose_id = int(link.get('href').split('/')[-1].replace('.html', ''))
            expose_ids.append(expose_id)
            if len(str(expose_id)) > 5:
                expose_urls.append('https://www.immobilienscout24.de/expose/' + str(expose_id))
            else:
                expose_urls.append(link.get('href'))

        attr_container_els = soup.find_all(
            lambda e: e.has_attr('data-is24-qa') and e['data-is24-qa'] == "attributes"
        )
        address_fields = soup.find_all(
            lambda e: e.has_attr('class') and 'result-list-entry__address' in e['class']
        )
        gallery_elements = soup.find_all(
            lambda e: e.has_attr('class') and 'result-list-entry__gallery-container' in e['class']
        )
        for idx, title_el in enumerate(title_elements):
            attr_els = attr_container_els[idx].find_all('dd')
            try:
                address = address_fields[idx].text.strip()
            except AttributeError:
                address = "No address given"

            gallery_tag = gallery_elements[idx].find("div", {"class": "gallery-container"})
            if gallery_tag is not None:
                image_tag = gallery_tag.find("img")
                try:
                    image = image_tag["src"]
                except KeyError:
                    image = image_tag["data-lazy-src"]
            else:
                image = None

            details = {
                'id': expose_ids[idx],
                'url': expose_urls[idx],
                'image': image,
                'title': title_el.text.strip().replace('NEU', ''),
                'address': address,
                'crawler': self.get_name()
            }
            if len(attr_els) > 2:
                details['price'] = attr_els[0].text.strip().split(' ')[0].strip()
                details['size'] = attr_els[1].text.strip().split(' ')[0].strip() + " qm"
                details['rooms'] = attr_els[2].text.strip().split(' ')[0].strip()
            else:
                # If there are less than three elements, it is unclear which is what.
                details['price'] = ''
                details['size'] = ''
                details['rooms'] = ''
            # print entries
            exist = False
            for expose in entries:
                if expose_id == expose["id"]:
                    exist = True
                    break
            if not exist:
                entries.append(details)

        logger.debug('Number of entries found: %d', len(entries))
        return entries
