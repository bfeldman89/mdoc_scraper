# !/usr/bin/env python
"""This module does blah blah."""
import re
import time
import unicodedata

from io import BytesIO
from urllib.parse import urljoin

import requests

from bs4 import BeautifulSoup
from PyPDF2 import PdfFileReader

from common import airtab_mdoc, airtab_mdoc2, dc, tw, muh_headers


def tweet_it(obj, tweet_txt):
    media_ids = []
    image_list = obj.normal_image_url_list[:2]
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
    full_txt_lines = this_dict['dc_p1_txt'].splitlines()
    if full_txt_lines[0] in {'COVID‐19 Confirmed Inmate Cases', 'COVID‐19 Confirmed Cases'}:
        this_dict['last_updated'] = full_txt_lines[-1].replace('Last Update:', '').replace('2020 ', '2020 at ').strip().replace('\x00', '')
        totals = re.findall(r'\d+', full_txt_lines[-2])
        this_dict['total_cases'] = totals[0]
        this_dict['tweet_msg'] = f"As of {this_dict['last_updated']}, a total of {this_dict['total_cases']} MS inmates have tested positive for COVID-19. {this_dict['dc_url']}"
        this_dict['tweet_id'] = tweet_it(obj, this_dict['tweet_msg'])
        new_rec = airtab_mdoc.insert(this_dict, typecast=True)
        time.sleep(5)
        scrape_covid_cases_per_facility(record_id=new_rec['id'])
    elif full_txt_lines[0].strip() == 'Answers to some of the most frequently asked questions:':
        this_dict['last_updated'] = full_txt_lines[1].replace('Last Update:', '').replace(', ', ', 2020 at ').strip().replace('\x00', '')
        scrape_q_and_a(this_dict)
    else:
        print('WTF! The first line of the pdf was: ', full_txt_lines[0].strip())


def scrape_q_and_a(this_dict):
    obj = dc.documents.get(int(this_dict['dc_id']))
    p2_txt = obj.get_page_text(2)
    txt_lines = p2_txt.splitlines()
    list_of_first_lines_of_answers = []
    for x in txt_lines:
        if x.startswith('A. '):
            first_line = x.replace('A. ', '').strip()
            list_of_first_lines_of_answers.append(first_line)
    excerpt = (
        f"\"{list_of_first_lines_of_answers[0].replace(' in the inmate', '')}.. "
        f"{list_of_first_lines_of_answers[2].replace(', based on the latest report', '')}.. "
        f"{list_of_first_lines_of_answers[3].replace(', based on the most available information', '')}.. "
        f"{list_of_first_lines_of_answers[4].replace('In addition to the positive cases, ', '')}\" "
    )
    this_dict['tweet_msg'] = f"As of {this_dict['last_updated']}, {excerpt} {this_dict['dc_url']}".replace('and', '&').replace('The department', 'MDOC')
    testing_data = re.findall(r"\d+", excerpt)
    this_dict['inmates_pos'] = testing_data[0]
    this_dict['inmates_pos_active'] = testing_data[1]
    this_dict['inmates_neg'] = testing_data[2]
    this_dict['staff_pos'] = testing_data[3]
    this_dict['staff_neg'] = testing_data[4]
    this_dict['tweet_id'] = tweet_it(obj, this_dict['tweet_msg'])
    airtab_mdoc.insert(this_dict, typecast=True)


def scrape_covid_cases_per_facility(record_id):
    rec = airtab_mdoc.get(record_id)
    this_dict = {}
    this_dict['iso'] = rec['fields']['iso']
    txt = rec['fields']['dc_p1_txt']
    lines = txt.splitlines()
    for line in lines:
        m = re.findall(r'\d+', line)
        if m:
            if len(m) == 2:
                cases = m[0]
                active_cases = m[1]
                facility = line.replace(cases, '').replace(active_cases, '').strip()
                this_dict[facility] = cases
                this_dict[f"{facility} (active)"] = active_cases
    airtab_mdoc2.insert(this_dict, typecast=True)


def main():
    url = 'https://www.mdoc.ms.gov/Pages/COVID-19-Information-and-Updates.aspx'
    r = requests.get(url, headers=muh_headers)
    soup = BeautifulSoup(r.text, 'html.parser')
    for link in soup.find_all('a'):
        this_dict = {'doc_type': 'covid_update'}
        relative_url = link.get('href')
        try:
            if relative_url.endswith('.pdf') and relative_url.startswith('/Documents/'):
                this_dict['url'] = urljoin(url, relative_url)
                # print(this_dict['url'])
                this_dict['raw_title'] = link.get_text(strip=True).replace('\u200b', '').replace(
                    '\xa0', '').replace('\x00', '').replace('CasesState', 'Cases: State')
                m = airtab_mdoc.match('url', this_dict['url'])
                if not m:
                    r = requests.get(this_dict['url'])
                    if r.status_code == 200:
                        web_to_dc(this_dict)
                    elif r.status_code == 401:
                        print('401 UNAUTHORIZED')
                else:
                    print('nothing new. nothing changed. --> ', this_dict['url'])
        except AttributeError:
            pass


def main_v2():
    urls = ['https://www.mdoc.ms.gov/Documents/covid-19/QA-Questions%20and%20Answers.pdf',
            'https://www.mdoc.ms.gov/Documents/covid-19/Inmates%20cases%20chart.pdf']
    for url in urls:
        response = requests.get(url)
        if response.status_code != 200:
            print(f'The url is broken. status code: {response.status_code}')
            return False
        this_dict = {'url': url}
        with BytesIO(response.content) as f:
            this_pdf = PdfFileReader(f)
            information = dict(this_pdf.getDocumentInfo())
        this_dict['pdf_author'] = information.get('/Author')
        this_dict['pdf_creator'] = information.get('/Creator')
        this_dict['pdf_mod_datetime'] = information.get('/ModDate').replace("'", '')
        this_dict['pdf_creation_datetime'] = information.get('/CreationDate').replace("'", '')
        this_dict['pdf_producer'] = information.get('/Producer')
        matching_record = airtab_mdoc.match('pdf_mod_datetime', this_dict['pdf_mod_datetime'])
        if matching_record:
            print('nothing new. nothing changed. --> ', this_dict['url'])
            return False
        web_to_dc(this_dict)

if __name__ == "__main__":
    main_v2()
