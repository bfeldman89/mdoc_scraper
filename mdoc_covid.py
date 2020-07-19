# !/usr/bin/env python
"""This module does blah blah."""
import time
import unicodedata
from io import BytesIO
from urllib.parse import urljoin
import requests
from bs4 import BeautifulSoup
from common import airtab_mdoc as airtab, dc, tw, muh_headers


def tweet_it(obj, tweet_txt):
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


def web_to_dc(this_dict):
    obj = dc.documents.upload(this_dict['url'])
    while obj.status != "success":
        time.sleep(7.5)
        obj = dc.documents.get(obj.id)
    obj.access = "public"
    obj.data = {'doc_type': 'covid_update'}
    obj.title = this_dict['raw_title']
    obj.source = 'MDOC'
    obj.put()
    time.sleep(5)
    this_dict['dc_id'] = str(obj.id)
    this_dict['dc_title'] = obj.title
    this_dict['dc_access'] = obj.access
    this_dict['dc_pages'] = obj.pages
    this_dict['dc_p1_txt'] = unicodedata.normalize("NFKD", obj.get_page_text(1))
    this_dict['dc_full_text'] = unicodedata.normalize("NFKD", obj.full_text)
    this_dict['dc_pdf'] = obj.pdf_url
    this_dict['dc_url'] = obj.canonical_url
    this_dict['dc_txt_url'] = obj.full_text_url
    full_txt_lines = this_dict['dc_full_text'].splitlines()
    if full_txt_lines[0] == 'COVID‐19 Confirmed Inmate Cases':
        this_dict['last_updated'] = full_txt_lines[-1].replace('Last Update:', '').replace('2020 ', '2020 at ').strip()
        this_dict['total_cases'] = full_txt_lines[-2].replace('TOTAL', '').strip()
        this_dict['tweet_msg'] = f"The online pdf of \"COVID-19 Confirmed Inmate Cases\" was updated on {this_dict['last_updated']}. The total number of *confirmed* cases is {this_dict['total_cases']}.\n{this_dict['dc_url']}"
        this_dict['tweet_id'] = tweet_it(obj, this_dict['tweet_msg'])
        airtab.insert(this_dict, typecast=True)


def main():
    url = 'https://www.mdoc.ms.gov/Pages/COVID-19-Information-and-Updates.aspx'
    r = requests.get(url, headers=muh_headers)
    soup = BeautifulSoup(r.text, 'html.parser')
    for link in soup.find_all('a'):
        this_dict = {'doc_type': 'covid_update'}
        relative_url = link.get('href')
        try:
            if relative_url.endswith('.pdf') and relative_url.startswith('/Documents/'):
                print(relative_url)
                this_dict['url'] = urljoin(url, relative_url)
                this_dict['raw_title'] = link.get_text(strip=True).replace('\u200b', '').replace('\xa0', '').replace('\x00', '').replace('CasesState', 'Cases: State')
                m = airtab.match('url', this_dict['url'])
                if not m:
                    r = requests.get(this_dict['url'])
                    if r.status_code == 200:
                        web_to_dc(this_dict)
        except AttributeError:
            pass


if __name__ == "__main__":
    main()
