# !/usr/bin/env python3
"""This module does blah blah."""
import os
import time
from io import BytesIO
from urllib.parse import urljoin, quote
import requests
import urllib3
from airtable import Airtable
from documentcloud import DocumentCloud
from bs4 import BeautifulSoup
from twython import Twython

tw = Twython(os.environ['TWITTER_APP_KEY'], os.environ['TWITTER_APP_SECRET'],
             os.environ['TWITTER_OAUTH_TOKEN'], os.environ['TWITTER_OAUTH_TOKEN_SECRET'])
dc = DocumentCloud(
    os.environ['DOCUMENT_CLOUD_USERNAME'], os.environ['DOCUMENT_CLOUD_PW'])
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
muh_headers = {
    'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.143 Safari/537.36'
}

airtab = Airtable(os.environ['other_scrapers_db'],
                  'mdoc', os.environ['AIRTABLE_API_KEY'])
airtab_log = Airtable(os.environ['log_db'],
                      'log', os.environ['AIRTABLE_API_KEY'])


def wrap_it_up(function, t0, new, total):
    this_dict = {'module': 'mdoc_scraper.py'}
    this_dict['function'] = function
    this_dict['duration'] = round((time.time() - t0) / 60, 2)
    this_dict['total'] = total
    this_dict['new'] = new
    airtab_log.insert(this_dict, typecast=True)


def scrape_daily_pop():
    """This function does blah blah."""
    t0, i = time.time(), 0
    url = 'https://www.mdoc.ms.gov/Admin-Finance/Pages/Daily-Inmate-Population.aspx'
    r = requests.get(url, headers=muh_headers)
    soup = BeautifulSoup(r.text, 'html.parser')
    rows = soup.find_all("td", class_="ms-vb")
    for row in rows[0:12]:
        this_dict = {'type': 'daily_pop'}
        this_dict['url'] = urljoin(url, quote(row.a.get('href')))
        this_dict['raw_title'] = row.string
        m = airtab.match('url', this_dict['url'], view='dp')
        if not m:
            i += 1
            dc_data = {'type': 'daily_pop'}
            obj = upload_to_documentcloud(this_dict['url'], this_dict, dc_data)
            new_record = airtab.insert(this_dict, typecast=True)
            tweet_txt = new_record['fields']['draft tweet']
            print(tweet_txt)
            this_dict['tweet_id'] = tweet_it(obj, tweet_txt)
            airtab.update(new_record['id'], this_dict, typecast=True)
    wrap_it_up('scrape_daily_pop', t0, i, 12)


def scrape_monthly_fact_scheets():
    """This function does blah blah."""
    t0, i = time.time(), 0
    url = 'https://www.mdoc.ms.gov/Admin-Finance/Pages/Monthly-Facts.aspx'
    r = requests.get(url, headers=muh_headers)
    soup = BeautifulSoup(r.text, 'html.parser')
    rows = soup.find_all("td", class_="ms-vb")
    for row in rows[0:12]:
        this_dict = {'type': 'mfs'}
        this_dict['url'] = urljoin(url, quote(row.a.get('href')))
        this_dict['raw_title'] = row.string
        m = airtab.match('url', this_dict['url'], view='mfs')
        if not m:
            i += 1
            dc_data = {'type': 'mfs'}
            obj = upload_to_documentcloud(this_dict['url'], this_dict, dc_data)
            new_record = airtab.insert(this_dict, typecast=True)
            tweet_txt = new_record['fields']['draft tweet']
            this_dict['tweet_id'] = tweet_it(obj, tweet_txt)
            airtab.update(new_record['id'], this_dict, typecast=True)
    wrap_it_up('scrape_monthly_fact_scheets', t0, i, 12)


def scrape_press_releases():
    """This function does blah blah."""
    t0, i = time.time(), 0
    url = 'https://www.mdoc.ms.gov/News/Pages/Press-Releases.aspx'
    r = requests.get(url, headers=muh_headers)
    soup = BeautifulSoup(r.text, 'html.parser')
    rows = soup.select("td.ms-vb > a[href]")
    for row in rows[0:12]:
        this_dict = {'type': 'pr'}
        this_dict['url'] = urljoin(url, quote(row.get('href')))
        this_dict['raw_title'] = row.string
        this_dict['date'] = row.parent.next_sibling.string
        m = airtab.match('url', this_dict['url'], view='pr')
        if not m:
            i += 1
            dc_data = {'type': 'pr'}
            obj = upload_to_documentcloud(this_dict['url'], this_dict, dc_data)
            new_record = airtab.insert(this_dict, typecast=True)
            tweet_txt = new_record['fields']['draft tweet']
            this_dict['tweet_id'] = tweet_it(obj, tweet_txt)
            airtab.update(new_record['id'], this_dict, typecast=True)
    wrap_it_up('scrape_press_releases', t0, i, 12)


def save_to_folder(this_dict):
    """save to folder"""
    r = requests.get(this_dict['url'], headers=muh_headers, verify=False)
    fn = this_dict['raw_title'] + '.pdf'
    with open(fn, 'wb') as f:
        f.write(r.content)
    time.sleep(2)
    return fn


def upload_to_documentcloud(pdf, this_dict, data):
    """upload to documnentcloud"""
    obj = dc.documents.upload(
        pdf, title=this_dict['raw_title'], source='MDOC', access='public', data=data)
    obj = dc.documents.get(obj.id)
    while obj.access != 'public':
        time.sleep(5)
        obj = dc.documents.get(obj.id)
    this_dict['dc_id'] = obj.id
    this_dict['dc_title'] = obj.title
    this_dict['dc_access'] = obj.access
    this_dict['dc_pages'] = obj.pages
    # this_dict['dc_full_text'] = obj.full_text.decode("utf-8")
    # this_dict['dc_pdf'] = obj.pdf_url
    return obj


def tweet_it(obj, tweet_txt):
    """tweet it"""
    media_ids = []
    image_list = obj.normal_image_url_list[:4]
    for image in image_list:
        r = requests.get(image)
        r.raise_for_status()
        uploadable = BytesIO(r.content)
        response = tw.upload_media(media=uploadable)
        media_ids.append(response['media_id'])
    tweet = tw.update_status(status=tweet_txt, media_ids=media_ids)
    return tweet['id_str']


def main():
    scrape_press_releases()
    scrape_monthly_fact_scheets()
    scrape_daily_pop()


if __name__ == "__main__":
    main()
