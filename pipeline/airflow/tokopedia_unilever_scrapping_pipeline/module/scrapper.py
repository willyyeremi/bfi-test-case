def run_firefox_script():
    import subprocess
    import time
    xvfb_process = subprocess.Popen(['Xvfb', ':99', '-screen', '0', '1920x1080x24'])
    time.sleep(5)

def driver_maker():
    # from selenium.webdriver import Chrome, ChromeOptions
    # from selenium.webdriver.chrome.service import Service
    # from webdriver_manager.chrome import ChromeDriverManager
    from selenium.webdriver import Firefox, FirefoxOptions
    from selenium.webdriver.firefox.service import Service
    # from webdriver_manager.firefox import GeckoDriverManager
    # from random_user_agent.user_agent import UserAgent
    # from random_user_agent.params import SoftwareName, OperatingSystem
    # service = Service(ChromeDriverManager().install())
    # service = Service(executable_path = "/home/airflow/chromedriver-linux64/chromedriver")
    # service = Service(GeckoDriverManager().install())
    service = Service(executable_path = "/home/airflow/geckodriver")
    # software_names = [SoftwareName.FIREFOX.value]
    # operating_systems = [OperatingSystem.WINDOWS.value, OperatingSystem.LINUX.value]
    # user_agent_rotator = UserAgent(software_names = software_names, operating_systems = operating_systems, limit = 100)
    # user_agent = user_agent_rotator.get_random_user_agent()
    # options =  ChromeOptions()
    options = FirefoxOptions()
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-gpu")
    options.add_argument('--display=:99')
    # options.add_argument("--window-size=1920x1080")
    # options.add_argument(f'user-agent={user_agent}')
    # driver = Chrome(service = service, options = options)
    driver = Firefox(service = service, options = options)
    return driver

def next_button_check(soup):
    next_button = soup.find('a', {'data-testid': 'btnShopProductPageNext', 'class': 'css-14ulvr4'})
    if next_button:
        next_page_url = next_button.get('href')
        next_page_url = "https://www.tokopedia.com" + next_page_url
    else:
        next_page_url = None
    return next_page_url

def item_link_get(soup):
    link_list = []
    product_containers = soup.find_all('div', class_='css-1sn1xa2')
    for product_container in product_containers:
        link_containers = product_container.find_all('a', class_='pcv3__info-content css-gwkf0u')
        for link_container in link_containers:
            product_url = link_container.get('href')
            link_list.append(product_url)
    return link_list

def shop_page_list_collect(url):
    from bs4 import BeautifulSoup
    import time
    import math
    driver = driver_maker()
    page_url_list = [url]
    current_url = url
    while current_url != None:
        driver.get(current_url)
        time.sleep(3)
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        next_page_url = next_button_check(soup)
        if next_page_url != None:
            page_url_list.append(next_page_url)
        current_url = next_page_url
    driver.quit()
    total_item_per_part = int(math.ceil(len(page_url_list) / 10))
    imaginary_total_item = total_item_per_part * 10
    start_offset = [i for i in range(0, imaginary_total_item, total_item_per_part)]
    end_offset = [i for i in range(total_item_per_part, imaginary_total_item + 1, total_item_per_part)]
    end_offset[-1] = len(page_url_list)
    dict_of_list_item = {}
    for i in range(0, len(start_offset)):
        part_item_list = page_url_list[start_offset[i]:end_offset[i]]
        dict_of_list_item[f'part_{i + 1}'] = part_item_list
    return dict_of_list_item

def product_list_loader(url_page_list):
    from bs4 import BeautifulSoup
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions
    import time
    import math
    driver = driver_maker()
    list_of_product_link = []
    for url in url_page_list:
        driver.get(url)
        time.sleep(3)
        # last_height = driver.execute_script("return document.body.scrollHeight")
        # while True:
        #     driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        #     time.sleep(3)
        #     new_height = driver.execute_script("return document.body.scrollHeight")
        #     if new_height == last_height:
        #         break
        #     last_height = new_height
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        link_list = item_link_get(soup)
        list_of_product_link.extend(link_list)
    driver.quit()
    total_item_per_part = int(math.ceil(len(list_of_product_link) / 10))
    imaginary_total_item = total_item_per_part * 10
    start_offset = [i for i in range(0, imaginary_total_item, total_item_per_part)]
    end_offset = [i for i in range(total_item_per_part, imaginary_total_item + 1, total_item_per_part)]
    end_offset[-1] = len(list_of_product_link)
    dict_of_list_item = {}
    for i in range(0, len(start_offset)):
        part_item_list = list_of_product_link[start_offset[i]:end_offset[i]]
        dict_of_list_item[f'part_{i + 1}'] = part_item_list
    return dict_of_list_item

def web_data_get(link_list):
    import bs4
    import time
    from datetime import datetime
    data_list = []
    driver = driver_maker()
    for link in link_list:
        current_timestamp = datetime.strftime(datetime.now(), '%Y-%m-%d')
        driver.get(link)
        time.sleep(3)
        soup = bs4.BeautifulSoup(driver.page_source, 'html.parser')
        product_data = {}
        product_data['name'] = soup.find('h1', class_='css-1xfedof').text.strip()
        product_data['type'] = soup.select('ol.css-60knpe > li.css-d5bnys')[3].text
        product_data['detail'] = soup.select_one('div[data-testid="lblPDPDescriptionProduk"]').text
        product_data['createdate'] = current_timestamp
        product_data['price'] = int(soup.select_one('div.price[data-testid="lblPDPDetailProductPrice"]').text.replace("Rp", "").replace(".", ""))
        discountpercentage_element = soup.select_one('span[data-testid="lblPDPDetailDiscountPercentage"]')
        if discountpercentage_element:
            product_data['discountpercentage'] = float(discountpercentage_element.text.replace("%", "")) / 100
        else:
            product_data['discountpercentage'] = float(0)
        originalprice_element = soup.select_one('span[data-testid="lblPDPDetailOriginalPrice"]')
        if originalprice_element:
            product_data['originalprice'] = int(originalprice_element.text.replace("Rp", "").replace(".", ""))
        else:
            product_data['originalprice'] = int(product_data['price'])
        product_data['platform'] = 'tokopedia'
        data_list.append(product_data)
    driver.quit()
    return data_list

def product_master_input(credential_path, data_list):
    from sqlalchemy import create_engine, MetaData, Column, Integer, String, Date
    from sqlalchemy.orm import sessionmaker, declarative_base
    from pandas import to_datetime
    import module
    database_credential_dict = module.connection.credential_get(credential_path)
    database_url = module.connection.url(database_credential_dict['user'], database_credential_dict['password'], database_credential_dict['host'], database_credential_dict['port'], database_credential_dict['database'])
    database_engine = create_engine(database_url)
    database_connection = database_engine.connect()
    database_session = sessionmaker(bind = database_engine)()
    metadata = MetaData()
    base = declarative_base(metadata = metadata)
    class productmaster(base):
        __tablename__ = 'productmaster'
        __table_args__ = {'schema': 'public'}
        id = Column(Integer, primary_key = True)
        name = Column(String)
        type = Column(String)
        detail = Column(String)
        createdate = Column(Date)
    for data in data_list:
        exist_check = database_session.query(productmaster).filter_by(name = data['name']).first()
        if exist_check:
            exist_check.type = data['type']
            exist_check.detail = data['detail']
        else:
            new_data = productmaster(
                name = data['name']
                ,type = data['type']
                ,detail = data['detail']
                ,createdate = to_datetime(data['createdate']))
            database_session.add(new_data)
            database_session.commit()
    exist_check = database_session.query(productmaster).filter_by(name = data['name']).first()
    database_connection.close()

def product_input(credential_path, data_list):
    from sqlalchemy import create_engine, MetaData, Column, ForeignKey, Integer, Float, String, Date
    from sqlalchemy.orm import sessionmaker, declarative_base
    from pandas import to_datetime
    import module
    database_credential_dict = module.connection.credential_get(credential_path)
    database_url = module.connection.url(database_credential_dict['user'], database_credential_dict['password'], database_credential_dict['host'], database_credential_dict['port'], database_credential_dict['database'])
    database_engine = create_engine(database_url)
    database_connection = database_engine.connect()
    database_session = sessionmaker(bind = database_engine)()
    metadata = MetaData()
    base = declarative_base(metadata = metadata)
    class productmaster(base):
        __tablename__ = 'productmaster'
        __table_args__ = {'schema': 'public'}
        id = Column(Integer, primary_key = True)
        name = Column(String)
        type = Column(String)
        detail = Column(String)
        createdate = Column(Date)
    class product(base):
        __tablename__ = 'product'
        __table_args__ = {'schema': 'public'}
        id = Column(Integer, primary_key = True)
        productmasterid = Column(Integer, ForeignKey(productmaster.id))
        price = Column(Integer)
        originalprice = Column(Integer)
        discountpercentage = Column(Float)
        platform = Column(String)
        createdate = Column(Date)
    for data in data_list:
        data['productmasterid'] = database_session.query(productmaster).filter_by(name = data['name']).first().id
        if data['price'] != 10000000:
            new_data = product(
                productmasterid = data['productmasterid']
                ,price = data['price']
                ,originalprice = data['originalprice']
                ,discountpercentage = data['discountpercentage']
                ,platform = data['platform']
                ,createdate = to_datetime(data['createdate']))
            database_session.add(new_data)
            database_session.commit()
    database_connection.close()

# x = shop_page_list_collect("https://www.tokopedia.com/unilever/product")