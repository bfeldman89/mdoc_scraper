# !/usr/bin/env python
"""This module does blah blah."""
import re
import time
import unicodedata
from io import BytesIO
from urllib.parse import urljoin
import requests
from bs4 import BeautifulSoup
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
    if full_txt_lines[0] == 'COVID‐19 Confirmed Inmate Cases':
        this_dict['last_updated'] = full_txt_lines[-1].replace('Last Update:', '').replace('2020 ', '2020 at ').strip().replace('\x00', '')
        this_dict['total_cases'] = full_txt_lines[-2].replace('TOTAL', '').strip()
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
            first_line = x.replace('A. ', '')
            first_line_trimmed = first_line[:first_line.find('.')+1]
            list_of_first_lines_of_answers.append(first_line_trimmed)
    excerpt = (
        f"\"{list_of_first_lines_of_answers[0].replace(', based on the most recent data', '')}.. "
        f"{list_of_first_lines_of_answers[2].replace(', based on the latest report', '')}.. "
        f"{list_of_first_lines_of_answers[3].replace(', based on the most available information', '')}.. "
        f"{list_of_first_lines_of_answers[4].replace('In addition to the positive cases, ', '')}\" "
    )
    this_dict['tweet_msg'] = f"As of {this_dict['last_updated']}, {excerpt} {this_dict['dc_url']}"
    testing_data = re.findall(r"\d+", excerpt)
    this_dict['inmates_pos'] = testing_data[0]
    this_dict['inmates_neg'] = testing_data[1]
    this_dict['staff_pos'] = testing_data[2]
    this_dict['staff_neg'] = testing_data[3]
    this_dict['tweet_id'] = tweet_it(obj, this_dict['tweet_msg'])
    airtab_mdoc.insert(this_dict, typecast=True)


def scrape_covid_cases_per_facility(record_id):
    rec = airtab_mdoc.get(record_id)
    this_dict = {}
    this_dict['iso'] = rec['fields']['iso']
    txt = rec['fields']['dc_p1_txt']
    lines = txt.splitlines()
    for line in lines:
        m = re.search(r'\d+$', line)
        if m:
            cases = m.group()
            facility = line.replace(cases, '').strip()
            this_dict[facility] = cases
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
                    print('nothing new. nothing changed. same ole shit. same ole fucking shit -->  ', relative_url)
        except AttributeError:
            pass


if __name__ == "__main__":
    main()
