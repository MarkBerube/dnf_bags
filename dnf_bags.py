# a dunfa scraper that grabs the top 100 damage dealers and inserts their data in a CSV file

import asyncio
import os
import pandas as pan
from jobs import JOBS as jobs
from requests_html import AsyncHTMLSession
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36',
    'Accept-Language': 'en-US, en;q=0.5'
}

CHROMEDRIVER_PATH = os.getenv('CHROMEDRIVER_PATH')
DUNFA_RANKING_URL = 'https://dunfaoff.com/ranking.df?jobName={}&jobGrowName={}&gender={}&page=1'
DUNFA_CHARACTER_URL = 'https://dunfaoff.com/SearchResult.df?server={}&characterid={}'

options = Options()
options.add_argument('--disable-web-security')
driver = webdriver.Chrome(CHROMEDRIVER_PATH, options=options)

# find all character ranking data
async def get_rank_data(session, url):
    r = await session.get(url)
    ranks = r.html.find('tr.character-row')
    data = []
    for row in ranks:
        rank_data = {
            'server': row.attrs['data-server'],
            'id': row.attrs['data-characterid']
        }
        data.append(rank_data)
    return data

# main func for ranking data
async def get_ranks(urls):
    asession = AsyncHTMLSession()
    tasks = (get_rank_data(asession, url) for url in urls)
    return await asyncio.gather(*tasks)

# convert the job data into URLs
urls = []
for job in jobs:
    for sub in job['subs']:
        urls.append(DUNFA_RANKING_URL.format(job['korean_name'], sub, job['gender']))

results = asyncio.run(get_ranks(urls))

# convert the ranking data into URLs
urls = []
for ranks in results:
    for char in ranks:
        urls.append(DUNFA_CHARACTER_URL.format(char['server'], char['id']))

dealers = []

# open up the char page in selenium
# selenium is required since this damage value is kept in JS
for url in urls:
    driver.implicitly_wait(1)
    driver.get(url)
    tag =  driver.find_element_by_id('damage_side').click()
    damage = driver.find_element_by_class_name("sinergeDmg0").text.replace(',','')

    dealers.append([url, damage])

# convert everything to a dataframe & save it as a CSV
df = pan.DataFrame(dealers, columns=['url', 'damage'])
df.sort_values(by=['damage'], ascending=True, inplace=True).to_csv('results/rankings.csv', index=False)
print("done.")