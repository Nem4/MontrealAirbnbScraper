from urllib.request import urlopen, Request
import linecache
# import json
import sys
import time
import urllib, json
from db import DBHelper
import random

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
def scrape(skip_plus = False, nb_pages=5):
    # current_url = 'https://www.airbnb.ca/s/Montreal--QC/homes?refinement_paths%5B%5D=%2Fhomes&adults=0&children=0&infants=0&guests=0&query=Montreal%2C%20QC&allow_override%5B%5D=&s_tag=G6_yvvwl'
    # current_url = 'https://www.airbnb.ca/api/v2/explore_tabs?version=1.4.5&satori_version=1.1.0&_format=for_explore_search_web&experiences_per_grid=20&items_per_grid=18&guidebooks_per_grid=20&auto_ib=true&fetch_filters=true&has_zero_guest_treatment=true&is_guided_search=true&is_new_cards_experiment=true&luxury_pre_launch=true&query_understanding_enabled=true&show_groupings=true&supports_for_you_v3=true&timezone_offset=-300&client_session_id=d324adc9-de58-48ad-8ec5-1e1f524f56c5&metadata_only=false&is_standard_search=true&refinement_paths%5B%5D=%2Fhomes&selected_tab_id=home_tab&map_toggle=true&allow_override%5B%5D=&zoom=9&search_by_map=true&sw_lat=45.13634436876523&sw_lng=-74.46736584130811&ne_lat=45.97280593149832&ne_lng=-72.79542759863322&s_tag=9-SzSNcx&section_offset=7&items_offset=54&last_search_session_id=1d156103-5eba-4851-9788-568be4ad340c&federated_search_session_id=a94d5dd5-6d93-4421-ae87-694a5c493ac0&screen_size=large&query=Montreal%2C%20QC&_intents=p1&key=d306zoyjsyarp7ifhu67rjxn52tv0t20&currency=CAD&locale=en-CA'
    try:

        # s = ParseMainXML(current_url, n)
        headers = {'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:65.0) Gecko/20100101 Firefox/65.0'}
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36'}
#Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36
        # cookie = {'bev': '1549292626_QUXDAXXxrfR7UlTi', 'affiliate' : 43720035, 'campaign': '.pi0.pk24883996964_97449571964_c_40203460644', 'affiliate_referral_at': 1549292626, 'last_aacb': '%7B%22af%22%3A%2243720035%22%2C%22c%22%3A%22.pi0.pk24883996964_97449571964_c_40203460644%22%2C%22timestamp%22%3A1549292626%2C%22gclid%22%3A%22EAIaIQobChMI6qSv4qyi4AIVRECGCh107w0MEAAYASAAEgLfavD_BwE%22%7D', 'jitney_client_session_id': 'c17e446c-7b1c-4bcc-bbe5-27ca1667465f', 'jitney_client_session_created_at': 1549417453, 'jitney_client_session_updated_at': 1549417685,
        #           '_user_attributes': '%7B%22curr%22%3A%22CAD%22%2C%22guest_exchange%22%3A1.31216%2C%22device_profiling_session_id%22%3A%221549292626--40263f7ed56c9225d35aa86d%22%2C%22giftcard_profiling_session_id%22%3A%221549417453--947fee2be81a04ef14e7ebfc%22%2C%22reservation_profiling_session_id%22%3A%221549417453--78bcab84f8a01d69e3b10413%22%7D'}
        #           # 'f4b08f5ff': 'treatment', 'cereal_exp': 26; 183ebdbbf=control; e8de742fb=control; __ssid=51dc471a63749533943a9786d29be46; 66bf01231=treatment; hyperloop_explore_exp_v2=5; cbkp=4; _gcl_au=1.1.404000349.1549292652; _ga=GA1.2.745079372.1549292653; _gid=GA1.2.1519692101.1549292653; a46dc25ab=short_upsell; 016951b48=control; e34ba1aae=control; sdid=; _csrf_token=V4%24.airbnb.ca%2478uBK57gIcQ%247KG43E7k7IUT2S3gyJDH-sT3oKzq1aSnXLjPyKlRbFA%3D; flags=0; AMP_TOKEN=%24NOT_FOUND; _gat=1; _gat_UA-2725447-1=1'
        districts = ['Ahuntsic-Cartierville', 'Anjou', 'Cote-des-Neiges-Notre-Dame-de-Grace', 'Lachine', 'LaSalle', 'Le-Plateau-Mont-Royal',
                    'Le-Sud-Ouest', "Ile-Bizard-Sainte-Genevieve", 'Mercier-Hochelaga-Maisonneuve', 'Montreal-Nord', 'Outremont', 'Pierrefonds-Roxboro',
                    'Riviere-des-Prairies-Pointe-aux-Trembles', 'Rosemont-La-Petite-Patrie', 'Saint-Laurent', 'Saint-Leonard', 'Verdun', 'Ville-Marie',
                    'Villeray-Saint-Michel-Parc-Extension']
        random.shuffle(districts)
        #
#GET /api/v2/explore_tabs?version=1.4.5&satori_version=1.1.0&_format=for_explore_search_web&experiences_per_grid=20&items_per_grid=18&guidebooks_per_grid=20&auto_ib=true&fetch_filters=true&has_zero_guest_treatment=true&is_guided_search=true&is_new_cards_experiment=true&luxury_pre_launch=false&query_understanding_enabled=true&show_groupings=true&supports_for_you_v3=true&timezone_offset=-300&client_session_id=cfefbe5c-ccbd-46c9-9ed2-8bbb2a8d8c89&metadata_only=false&is_standard_search=true&refinement_paths%5B%5D=%2Fhomes&selected_tab_id=home_tab&adults=1&children=0&infants=0&guests=6&place_id=ChIJDbdkHFQayUwR7-8fITgxTmU&screen_size=large&query=Montréal-Nord%2C%20QC&_intents=p1&key=d306zoyjsyarp7ifhu67rjxn52tv0t20&currency=CAD&locale=en-CA HTTP/1.1
        # print(str(len(districts)))
        for district in districts:
            print(f"######################################################################## DISTRICT: {district}  ########################################################################")
            # print(str(district))
            # https://www.airbnb.ca/api/v2/explore_tabs?version=1.4.5&satori_version=1.1.0&_format=for_explore_search_web&experiences_per_grid=20&items_per_grid=18&guidebooks_per_grid=20&auto_ib=false&fetch_filters=true&has_zero_guest_treatment=true&is_guided_search=true&is_new_cards_experiment=true&luxury_pre_launch=false&query_understanding_enabled=true&show_groupings=true&supports_for_you_v3=true&timezone_offset=-300&client_session_id=0ec30567-e89f-42a3-9114-1b0025986647&metadata_only=false&is_standard_search=true&refinement_paths%5B%5D=%2Fhomes&selected_tab_id=home_tab&adults=4&children=0&infants=0&guests=0&map_toggle=true&allow_override%5B%5D=&ib=true&zoom=14&search_by_map=true&s_tag=L4pZXxGZ&screen_size=medium&query=Downtown%2C%20Montreal%2C%20QC&_intents=p1&key=d306zoyjsyarp7ifhu67rjxn52tv0t20&currency=CAD&locale=en-CA
            for page in range(0, nb_pages):
                try:
                    print(f"************************* PAGE # {page}  *************************")
                    if page == 0:
                        current_url = f'https://www.airbnb.ca/api/v2/explore_tabs?version=1.4.5&satori_version=1.1.0&_format=for_explore_search_web&experiences_per_grid=20&items_per_grid=18&guidebooks_per_grid=20&auto_ib=true&fetch_filters=true&has_zero_guest_treatment=true&is_guided_search=true&is_new_cards_experiment=true&luxury_pre_launch=false&query_understanding_enabled=true&show_groupings=true&supports_for_you_v3=true&timezone_offset=-300&client_session_id=cfefbe5c-ccbd-46c9-9ed2-8bbb2a8d8c89&metadata_only=false&is_standard_search=true&refinement_paths%5B%5D=%2Fhomes&selected_tab_id=home_tab&adults=1&children=0&infants=0&guests=6&place_id=ChIJDbdkHFQayUwR7-8fITgxTmU&screen_size=large&query={str(district)}%2C%20QC&_intents=p1&key=d306zoyjsyarp7ifhu67rjxn52tv0t20&currency=CAD&locale=en-CA'
                    else:
                        item_offset = page * 18
                        current_url = f'https://www.airbnb.ca/api/v2/explore_tabs?version=1.4.5&satori_version=1.1.0&_format=for_explore_search_web&experiences_per_grid=20&items_per_grid=18&guidebooks_per_grid=20&auto_ib=true&fetch_filters=true&has_zero_guest_treatment=true&is_guided_search=true&is_new_cards_experiment=true&luxury_pre_launch=true&query_understanding_enabled=true&show_groupings=true&supports_for_you_v3=true&timezone_offset=-300&client_session_id=c17e446c-7b1c-4bcc-bbe5-27ca1667465f&metadata_only=false&is_standard_search=true&refinement_paths%5B%5D=%2Fhomes&selected_tab_id=home_tab&adults=1&children=0&infants=0&guests=1&map_toggle=true&allow_override%5B%5D=&zoom=13&search_by_map=true&s_tag=ZiZuTulV&section_offset=7&items_offset={item_offset}&last_search_session_id=7b33aef3-0fd9-4cbc-a73a-454d4ac8e66b&federated_search_session_id=0dc88c66-5572-40f9-9e9b-ac0e99c3f0da&screen_size=large&query={str(district)}%2C%20QC&_intents=p1&key=d306zoyjsyarp7ifhu67rjxn52tv0t20&currency=CAD&locale=en-CA'
                    print(f"current URL {current_url}")
                    req = Request(url=current_url, headers=headers)
                    # print(f"ENCODING: {req.encoding}")
                    # req.encoding = 'UTF-8'
                    data = urlopen(req).read()
                    # print("Data: " + str(data))
                    curr_data = json.loads(data)
                    # print(f"Data: {curr_data}")

                    i = 0
                    for k in range(0, len(curr_data['explore_tabs'][0]['sections'])):

                        try:
                            if len(curr_data['explore_tabs'][0]['sections'][k]['listings']) > 0:
                                while (i < len(curr_data['explore_tabs'][0]['sections'][k]['listings'])):
                                    # for item in curr_data['explore_tabs'][0]['sections'][1]['listings']:
                                    item = curr_data['explore_tabs'][0]['sections'][k]['listings'][i]
                                    if skip_plus:
                                        if 'kicker_badge' in item['listing']['kicker_content']:
                                            print('Skipped Plus listing')
                                            i += 1
                                            continue
                                    # print(f"https://airbnb.ca/rooms/{item['listing']['id']}")
                                    # print(f"price {i} : ${item['pricing_quote']['rate']['amount']}")
                                    save_item(item, page, district)
                                    print("Saved new item")
                                    i += 1
                                # time.sleep(random.randint(13,20))

                        except:
                            print_exception()
                except:
                    print_exception()
                # time.sleep(6)
                time.sleep(random.randint(10, 15))
    except:
        print_exception()


def save_item(item, page, district):
    #region try Block
    try:
        room_id = item['listing']['id']
        print(f"room id: {room_id}")
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
        'district': district

    }

    db.insert_item(item_data)
    db.insert_amenities(item['listing']['amenity_ids'], roomID=item['listing']['id'])


def test():
    test_url = 'https://www.airbnb.ca/s/Montreal--QC/homes?refinement_paths%5B%5D=%2Fhomes&query=Montreal%2C%20QC&adults=6&children=0&infants=0&guests=6&place_id=ChIJDbdkHFQayUwR7-8fITgxTmU&allow_override%5B%5D=&s_tag=Z2jGTIzN'
    import requests as req

    resp = req.get(test_url)
    print(resp.text)
    print(str(resp.json))
    # headers = {'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:65.0) Gecko/20100101 Firefox/65.0'}
    # with urllib.request.urlopen(test_url) as url:
    #     data = json.load(url)
    #     # data = json.loads(url.read().decode())
    #     print(data)


scrape(skip_plus=False, nb_pages=5)

# db.setup()
# test()
