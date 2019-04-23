from urllib.request import urlopen, Request
import linecache
# import json
import sys
import time
import urllib, json
from db import DBHelper
import random
from decimal import *

def print_exception():
    exc_type, exc_obj, tb = sys.exc_info()
    f = tb.tb_frame
    lineno = tb.tb_lineno
    filename = f.f_code.co_filename
    linecache.checkcache(filename)
    line = linecache.getline(filename, lineno, f.f_globals)
    print('EXCEPTION IN ({}, LINE {} "{}"): {}'.format(filename, lineno, line.strip(), exc_obj))
    print(str(sys.exc_info()))


db = DBHelper()
# db.setup()


def count_words(string):
    import re
    count = len(re.findall("[a-zA-Z_]+", string))
    print(f"COUNT: {count}")
    return count
    # while Trueunt)
    # print("Terminated")


def scrape(skip_plus = False, nb_pages=5):
    try:
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36'}
        districts = ['Montreal-East', 'Kirkland', 'Dollard-Des-Ormeaux', 'Dorval', 'Pierrefonds', 'Beaconsfield', 'Pointe-Claire','Montreal', 'Ahuntsic-Cartierville', 'Anjou', 'Cote-des-Neiges-Notre-Dame-de-Grace', 'Lachine', 'LaSalle', 'Le-Plateau-Mont-Royal',
                    'Le-Sud-Ouest', "Ile-Bizard-Sainte-Genevieve", 'Mercier-Hochelaga-Maisonneuve', 'Montreal-Nord', 'Outremont', 'Pierrefonds-Roxboro',
                    'Riviere-des-Prairies-Pointe-aux-Trembles', 'Rosemont-La-Petite-Patrie', 'Saint-Laurent', 'Saint-Leonard', 'Verdun', 'Ville-Marie',
                    'Villeray-Saint-Michel-Parc-Extension']
        random.shuffle(districts)
        for rep in range (0, 5):
            for district in districts:
                print(f"######################################################################## DISTRICT: {district}  ########################################################################")

                already_failed = False
                for page in range(0, nb_pages):
                    try:
                        print(f"************************* PAGE # {page}  *************************")
                        item_offset = page * 18
                        map_zoom = 8 + rep*2
                        current_url = f"https://www.airbnb.ca/api/v2/explore_tabs?version=1.4.5&satori_version=1.1.0&_format=for_explore_search_web&experiences_per_grid=20&items_per_grid=18&guidebooks_per_grid=20&auto_ib=true&fetch_filters=true&has_zero_guest_treatment=true&is_guided_search=true&is_new_cards_experiment=true&luxury_pre_launch=true&query_understanding_enabled=true&show_groupings=true&supports_for_you_v3=true&timezone_offset=-300&metadata_only=false&is_standard_search=true&refinement_paths%5B%5D=%2Fhomes&selected_tab_id=home_tab&adults=4&children=0&infants=0&guests=0&ib=true&map_toggle=true&allow_override%5B%5D=&zoom={map_zoom}&search_by_map=true&s_tag=Yy8g-Vvk&section_offset=7&items_offset={item_offset}&screen_size=large&query={district}%2C%20Montreal%2C%20QC&_intents=p1&key=d306zoyjsyarp7ifhu67rjxn52tv0t20&currency=CAD&locale=en-CA"
                        print(f"current URL {current_url}")
                        req = Request(url=current_url, headers=headers)
                        data = urlopen(req).read()
                        curr_data = json.loads(data)

                        i = 0
                        should_break = False
                        print(f"Section len: {len(curr_data['explore_tabs'][0]['sections'])}")
                        print(f"Section: {curr_data['explore_tabs'][0]['sections']}")
                        for k in range(0, len(curr_data['explore_tabs'][0]['sections'])):
                            try:
                                if 'listings' in curr_data['explore_tabs'][0]['sections'][k] and len(curr_data['explore_tabs'][0]['sections'][k]['listings']) > 0:
                                    while (i < len(curr_data['explore_tabs'][0]['sections'][k]['listings'])):
                                        # for item in curr_data['explore_tabs'][0]['sections'][1]['listings']:
                                        item = curr_data['explore_tabs'][0]['sections'][k]['listings'][i]
                                        if skip_plus:
                                            if 'kicker_badge' in item['listing']['kicker_content']:
                                                print('Skipped Plus listing')
                                                i += 1
                                                continue
                                        try:
                                            save_item(item, page, district)
                                            print("Saved new item")
                                        except:
                                            print_exception()

                                        i += 1
                                else:
                                    if k>0 or len(curr_data['explore_tabs'][0]['sections']) == 1:
                                        should_break = True
                                        already_failed = True
                                        break
                            except:
                                already_failed = True
                                print_exception()
                        if should_break or already_failed:
                            break
                    except:
                        print_exception()
                    time.sleep(random.randint(10, 15))
            time.sleep(20)
    except:
        print_exception()


def save_item(item, page, district):
    # region try Block
    try:
        room_id = item['listing']['id']
    except:
        print("cant get room_id")
        room_id = None
    try:
        bathroom_label = item['listing']['bathroom_label']
    except:
        bathroom_label = None
    try:
        bathrooms = item['listing']['bathrooms']
    except:
        bathrooms = None
    try:
        bed_label = item['listing']['bed_label']
    except:
        bed_label = None
    try:
        beds = item['listing']['beds']
    except:
        beds = None
    try:
        bedroom_label = item['listing']['bedroom_label']
    except:
        bedroom_label = None
    try:
        bedrooms = item['listing']['bedrooms']
    except:
        bedroom = None
    try:
        guest_label = item['listing']['guest_label']
    except:
        guest_label = None
    try:
        is_business_travel_ready = item['listing']['is_business_travel_ready']
    except:
        is_business_travel_ready = None
    try:
        is_fully_refundable = item['listing']['is_fully_refundable']
    except:
        is_fully_refundable = None
    try:
        is_host_highly_rated = item['listing']['is_host_highly_rated']
    except:
        is_host_highly_rated = None
    try:
        is_new_listing = item['listing']['is_new_listing']
    except:
        is_new_listing = None
    try:
        is_superhost = item['listing']['is_superhost']
    except:
        is_superhost = None
    try:
        lat = item['listing']['lat']
    except:
        lat = None
    try:
        lng = item['listing']['lng']
    except:
        lng = None
    try:
        localized_neighborhood = item['listing']['localized_neighborhood']
    except:
        localized_neighborhood = None
    try:
        name = item['listing']['name']
    except:
        name= None
    try:
        neighborhood = item['listing']['neighborhood']
    except:
        neighborhood= None
    try:
        person_capacity = item['listing']['person_capacity']
    except:
        person_capacity= None
    try:
        picture_count = item['listing']['picture_count']
    except:
        picture_count= None
    try:
        property_type_id = item['listing']['property_type_id']
    except:
        property_type_id= None
    try:
        reviews_count = item['listing']['reviews_count']
    except:
        reviews_count = None
    try:
        room_and_property_type = item['listing']['room_and_property_type']
    except:
        room_and_property_type = None
    try:
        room_type = item['listing']['room_type']
    except:
        room_type = None
    try:
        space_type = item['listing']['space_type']
    except:
        space_type = None
    try:
        star_rating = item['listing']['star_rating']
    except:
        star_rating = None
    try:
        tier_id = item['listing']['tier_id']
    except:
        tier_id = None
    try:
        amenity_ids = item['listing']['amenity_ids']
    except:
        amenity_ids = None
    try:
        avg_rating = item['listing']['avg_rating']
    except:
        avg_rating = None
    # pricing quote
    try:
        can_instant_book = item['pricing_quote']['can_instant_book']
    except:
        can_instant_book = None
    try:
        monthly_price_factor = item['pricing_quote']['monthly_price_factor']
    except:
        monthly_price_factor = None
    try:
        amount = item['pricing_quote']['rate']['amount']
    except:
        amount = None
    try:
        rate_type = item['pricing_quote']['rate_type']
    except:
        rate_type = None
    # verified
    try:
        enabled = item['verified']['enabled']
    except:
        enabled = None

    try:
        amenity_ids = item['listing']['amenity_ids']
        amenity_ids.sort()
    except:
        amenity_ids = None

    try:
        words = ""
        words += " " + item['listing']['neighborhood_overview']
        words += " " + item['listing']['space']
        words += " " + item['listing']['summary']
    except:
        words = ""
    # page
    #endregion
    item_data = {
    # listing
        'room_id': room_id,
        'bathroom_label': bathroom_label,
        'bathrooms': bathrooms,
        'bed_label': bed_label,
        'beds': beds,
        'bedroom_label': bedroom_label,
        'bedrooms': bedrooms,
        'guest_label': guest_label,
        'is_business_travel_ready': is_business_travel_ready,
        'is_fully_refundable': is_fully_refundable,
        'is_host_highly_rated': is_host_highly_rated,
        'is_new_listing': is_new_listing,
        'is_superhost': is_superhost,
        'lat': lat,
        'lng': lng,
        'localized_neighborhood': localized_neighborhood,
        'name': name,
        'neighborhood': neighborhood,
        'person_capacity': person_capacity,
        'picture_count': picture_count,
        'property_type_id': property_type_id,
        'reviews_count': reviews_count,
        'room_and_property_type': room_and_property_type,
        'room_type': room_type,
        'space_type': space_type,
        'star_rating': star_rating,
        'tier_id': tier_id,
        # 'amenity_ids': amenity_ids,
        'avg_rating': avg_rating,
    # pricing quote
        'can_instant_book': can_instant_book,
        'monthly_price_factor': monthly_price_factor,
        'amount': amount,
        'rate_type': rate_type,
    # verified
        'enabled': enabled,
    # page
        'page': page,
        'district': district,
        'n_of_words': count_words(words),
        'amenity_ids': amenity_ids
    }

    db.insert_item(item_data)


def test():
    test_url = 'https://www.airbnb.ca/s/Montreal--QC/homes?refinement_paths%5B%5D=%2Fhomes&query=Montreal%2C%20QC&adults=6&children=0&infants=0&guests=6&place_id=ChIJDbdkHFQayUwR7-8fITgxTmU&allow_override%5B%5D=&s_tag=Z2jGTIzN'
    import requests as req

    resp = req.get(test_url)
    print(resp.text)
    print(str(resp.json))


scrape(skip_plus=False, nb_pages=20)
