import records
import sys
import linecache
import psycopg2
import datetime as dt

def print_exception():
    exc_type, exc_obj, tb = sys.exc_info()
    f = tb.tb_frame
    lineno = tb.tb_lineno
    filename = f.f_code.co_filename
    linecache.checkcache(filename)
    line = linecache.getline(filename, lineno, f.f_globals)
    print('EXCEPTION IN ({}, LINE {} "{}"): {}'.format(filename, lineno, line.strip(), exc_obj))

# pip install psycopg2-binary
# users_ids = {612111589, 421424722, 326997765}
class DBHelper:

    def __init__(self, db_name="airbnb"):
        self.db = records.Database(f'postgresql://localhost/{db_name}?user=airbnb&password=kek123321')
        # self.db.query('SELECT pg_terminate_backend(pg_stat_activity.pid) FROM pg_stat_activity WHERE datname = current_database() AND pid <> pg_backend_pid();')
        print("Connected to DB")

    def setup(self):
        try:
            self.db.query('DROP TABLE room')
            self.db.query('DROP TABLE amenity')
        except:
            print("Table images does not exist")
        print("deleted tables")
        # self.db.query("CREATE TABLE test (nom varchar(50))")
        room_table_creation_sql = '''
CREATE TABLE IF NOT EXISTS room (room_id INTEGER UNIQUE, bathroom_label VARCHAR(120), bathrooms  INTEGER, bed_label  VARCHAR(120), beds  INTEGER, bedroom_label  VARCHAR(120),
bedrooms  INTEGER, guest_label VARCHAR(120), is_business_travel_ready BOOLEAN, is_fully_refundable BOOLEAN, is_host_highly_rated BOOLEAN,
is_new_listing BOOLEAN, is_superhost BOOLEAN, lat DECIMAL(10,5), lng DECIMAL(10,5), localized_neighborhood VARCHAR(120), name VARCHAR(120), 
neighborhood VARCHAR(120), person_capacity INTEGER, picture_count INTEGER, property_type_id INTEGER, reviews_count INTEGER, room_and_property_type VARCHAR(120),
room_type VARCHAR(120), space_type VARCHAR(120), star_rating DECIMAL(5,2), tier_id INTEGER, avg_rating DECIMAL(5,2), can_instant_book BOOLEAN,
monthly_price_factor DECIMAL(5,2), amount DECIMAL(5,2), rate_type VARCHAR(120), enabled BOOLEAN, page INTEGER, district VARCHAR(120)'''
        for i in range(0,60):
            room_table_creation_sql += f", amenity_{i} BOOLEAN"
        room_table_creation_sql += ")"

        self.db.query(room_table_creation_sql)

        self.db.query('CREATE TABLE IF NOT EXISTS amenity (room_id INTEGER, amentiy_id INTEGER, amenity VARCHAR(120), amenity_description VARCHAR(120))')
        print("Created tables")


    def list_rooms(self):
        select_query = 'SELECT * FROM room'

        rooms = self.db.query(select_query)
        for room in rooms:
            print(str(room))

    def insert_item(self, item):
        # insert_query = f"INSERT INTO room ("
        # for param in item:
        #     insert_query += f"{param}, "
        # insert_query = insert_query[: -2] + ") values("
        insert_query = "INSERT INTO room(room_id, bathroom_label, bathrooms, bed_label, beds, bedroom_label, bedrooms, guest_label, is_business_travel_ready, is_fully_refundable, is_host_highly_rated, is_new_listing, is_superhost, lat, lng, localized_neighborhood, name, neighborhood, person_capacity, picture_count, property_type_id, reviews_count, room_and_property_type, room_type, space_type, star_rating, tier_id, avg_rating, can_instant_book, monthly_price_factor, amount, rate_type, enabled, page, district"
        for i in range(0,60):
            insert_query+= f", amenity_{i}"
        insert_query += ") VALUES ("
        print(f"Page#: {item['page']}")
        for param in item:
            # if param == 'page':
            #     insert_query += f"{item[str(param)]}, "
            #     # parameter_to_insert = item[str(param)]
            # else:
            #     parameter_to_insert = item[str(param)]
            parameter_to_insert = item[str(param)]
            if param == "amenity_ids":
                insert_query = insert_query[:-2]
                for i in range(0,60):
                    insert_query += ", "
                    if i in parameter_to_insert:
                        insert_query += 'True'
                    else:
                        insert_query += 'False'
            else:

                try:
                    parameter_to_insert = parameter_to_insert.replace('\'', '-')
                except:
                    # print("Not a string")
                    pass

                if isinstance(parameter_to_insert, str):
                    str_to_insert = parameter_to_insert
                    str_to_insert.replace('\'', '-')
                    # str_to_insert.
                    import unidecode
                    unaccented_string = unidecode.unidecode(str_to_insert)
                    insert_query += f"'{unaccented_string}', "
                elif parameter_to_insert == None:
                    insert_query += f"NULL, "
                else:
                    insert_query += f"{parameter_to_insert}, "


        # insert_query += ')'
        # insert_query = insert_query[:-2] + ")" \
        insert_query += ")" \
                                           " ON CONFLICT (room_id) DO NOTHING"
        # print(f"Insert Query: {insert_query}")
        self.db.query(insert_query)

    def insert_amenities(self, amenities, roomID):

        for amenity in amenities:
            insert_sql = f"INSERT INTO amenity (room_id, amentiy_id, amenity, amenity_description) VALUES "
            # insert_sql+=

    def test(self):
        self.db.query("INSERT INTO room(room_id, bathroom_label, bathrooms, bed_label, beds, bedroom_label, bedrooms, guest_label, is_business_travel_ready, is_fully_refundable, is_host_highly_rated, is_new_listing, is_superhost, lat, lng, localized_neighborhood, name, neighborhood, person_capacity, picture_count, property_type_id, reviews_count, room_and_property_type, room_type, space_type, star_rating, tier_id, avg_rating, can_instant_book, monthly_price_factor, amount, rate_type, enabled, page) VALUES (31164170, '1 bath', 1.0, '1 bed', 1, '1 bedroom', 1, '3 guests', False, True, False, True, False, 45.51793, -73.65559, 'Mont-Royal', 'Entire 1 bedroom apt. in Town Mount-Royal', 'Mont-Royal', 3, 11, 1, 2, 'Entire apartment', 'Entire home/apt', 'Entire apartment', NULL, 0, 5.0, True, 0.8, 40.0, 'nightly', False, 1)")

    def get_avg_price_by_page(self, page):
        query = f"SELECT AVG(amount) FROM room WHERE page = {page}"
        avg_price = self.db.query(query)[0]['avg']
        print(f"Avg price #{page}: {avg_price}")
        return avg_price


# db = DBHelper()
# db.get_avg_price_by_page(0)
# db.get_avg_price_by_page(1)
# db.get_avg_price_by_page(2)
# db.get_avg_price_by_page(3)
# db.get_avg_price_by_page(4)
# db.get_avg_price_by_page(5)
# db.get_avg_price_by_page(6)
# db.test()
# db.setup()
# db.list_rooms()
