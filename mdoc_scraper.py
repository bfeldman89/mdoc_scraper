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


def scrape_daily_pop(ifttt_payload):
    """This function does blah blah."""
    url = 'https://www.mdoc.ms.gov/Admin-Finance/Pages/Daily-Inmate-Population.aspx'
    r = requests.get(url, headers=muh_headers)
    soup = BeautifulSoup(r.text, 'html.parser')
    rows = soup.find_all("td", class_="ms-vb")
    i = 0
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
    ifttt_payload['Value1'] += i
    ifttt_payload['Value2'] += len(rows)


def scrape_monthly_fact_scheets(ifttt_payload):
    """This function does blah blah."""
    url = 'https://www.mdoc.ms.gov/Admin-Finance/Pages/Monthly-Facts.aspx'
    r = requests.get(url, headers=muh_headers)
    soup = BeautifulSoup(r.text, 'html.parser')
    rows = soup.find_all("td", class_="ms-vb")
    i = 0
    for row in rows[0:10]:
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
    ifttt_payload['Value1'] += i
    ifttt_payload['Value2'] += len(rows)


def scrape_press_releases(ifttt_payload):
    """This function does blah blah."""
    url = 'https://www.mdoc.ms.gov/News/Pages/Press-Releases.aspx'
    r = requests.get(url, headers=muh_headers)
    soup = BeautifulSoup(r.text, 'html.parser')
    rows = soup.select("td.ms-vb > a[href]")
    i = 0
    for row in rows[0:10]:
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
    ifttt_payload['Value1'] += i
    ifttt_payload['Value2'] += len(rows)


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
    t0 = time.time()
    ifttt_payload = {'Value1': 0, 'Value2': 0}
    scrape_press_releases(ifttt_payload)
    scrape_monthly_fact_scheets(ifttt_payload)
    scrape_daily_pop(ifttt_payload)
    ifttt_payload['Value3'] = round(time.time() - t0, 2)
    ifttt_event_url = os.environ['IFTTT_WEBHOOKS_URL'].format('mdoc_scraper')
    requests.post(ifttt_event_url, json=ifttt_payload)


if __name__ == "__main__":
    main()
