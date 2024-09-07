import module
from pathlib import Path


credential_path = f'{Path(__file__).parent.parent.parent.parent}\\credential.csv'
# credential_path = '/home/airflow/credential.csv'

state = 1
url = 'https://www.tokopedia.com/unilever/product'

while state == 1:
    link_list, url, state = module.scrapper.product_list_loader(url)
    data_list = module.scrapper.web_data_get(link_list)
    module.scrapper.product_master_input(credential_path, data_list)
    module.scrapper.product_input(credential_path, data_list)