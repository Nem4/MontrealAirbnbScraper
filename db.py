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
        self.db.query('CREATE TABLE IF NOT EXISTS room (room_id INTEGER, bathroom_label VARCHAR(80), bathrooms  INTEGER, bed_label  VARCHAR(80), beds  INTEGER, bedroom_label  VARCHAR(80), ' \
    'bedrooms  INTEGER, guest_label VARCHAR(80), is_business_travel_ready BOOLEAN, is_fully_refundable BOOLEAN, is_host_highly_rated BOOLEAN, ' \
    'is_new_listing BOOLEAN, is_superhost BOOLEAN, lat DECIMAL(10,5), lng DECIMAL(10,5), localized_neighborhood VARCHAR(80), name VARCHAR(80), ' \
    'neighborhood VARCHAR(80), person_capacity INTEGER, picture_count INTEGER, property_type_id INTEGER, reviews_count INTEGER, room_and_property_type VARCHAR(80), ' \
    'room_type VARCHAR(80), space_type VARCHAR(80), star_rating DECIMAL(5,2), tier_id INTEGER, avg_rating DECIMAL(5,2), can_instant_book BOOLEAN,' \
    'monthly_price_factor DECIMAL(5,2), amount DECIMAL(5,2), rate_type VARCHAR(80), enabled BOOLEAN, page INTEGER)')

        self.db.query('CREATE TABLE IF NOT EXISTS amenity (room_id INTEGER, amentiy_id INTEGER, amenity VARCHAR(80), amenity_description VARCHAR(120))')
        print("Created tables")


    def list_rooms(self):
        select_query = 'SELECT * FROM room'

        rooms = self.db.query(select_query)
        for room in rooms:
            print(str(room))

    def insert_item(self, item):
        insert_query = f"INSERT INTO room ("
        for param in item:
            insert_query += f"{param}, "
        insert_query = insert_query[: -2] + ") values("
        for param in item:
            if isinstance(item[str(param)], str):
                insert_query += f"'{item[str(param)]}', "
            elif isinstance(item[str(param)], bool):
                insert_query += f"{item[str(param)]}, "
            else:
                insert_query += f"{item[str(param)]}, "
        # insert_query += ')'
        insert_query = insert_query[:-2] + ")"
        print(f"Insert Query: {insert_query}")
        self.db.query(insert_query)
        # insert_query = f"INSERT INTO room values({item['room_id']}, {item['bathroom_label']}, {item['bathrooms']}, {item['room_id']}, {item['bed_label']}, {item['room_id']}, {item['room_id']}, {item['room_id']}, {item['room_id']}" \
        #     f", {item['room_id']}, {item['room_id']}, {item['room_id']}, {item['room_id']}, {item['room_id']}, {item['room_id']}, {item['room_id']}" \
        #     f", {item['room_id']}, {item['room_id']}, {item['room_id']}, {item['room_id']}, {item['room_id']}, {item['room_id']}, {item['room_id']}" \
        #     f", {item['room_id']}, {item['room_id']}, {item['room_id']}, {item['room_id']}, {item['room_id']}, {item['room_id']}, {item['room_id']}" \
        #     f", {item['room_id']}, {item['room_id']}, {item['room_id']}, {item['room_id']}, {item['room_id']}, {item['room_id']})"
        # 'room_id': item['listing']['id'],
        # 'bathroom_label': item['listing']['bathroom_label'],
        # 'bathrooms': item['listing']['bathrooms'],
        # 'bed_label': item['listing']['bed_label'],
        # 'beds': item['listing']['beds'],
        # 'bedroom_label': item['listing']['bedroom_label'],
        # 'bedrooms': item['listing']['bedrooms'],
        # 'guest_label': item['listing']['guest_label'],
        # 'is_business_travel_ready': item['listing']['is_business_travel_ready'],
        # 'is_fully_refundable': item['listing']['is_fully_refundable'],
        # 'is_host_highly_rated': item['listing']['is_host_highly_rated'],
        # 'is_new_listing': item['listing']['is_new_listing'],
        # 'is_superhost': item['listing']['is_superhost'],
        # 'lat': item['listing']['lat'],
        # 'lng': item['listing']['lng'],
        # 'localized_neighborhood': item['listing']['localized_neighborhood'],
        # 'name': item['listing']['name'],
        # 'neighborhood': item['listing']['neighborhood'],
        # 'person_capacity': item['listing']['person_capacity'],
        # 'picture_count': item['listing']['picture_count'],
        # 'property_type_id': item['listing']['property_type_id'],
        # 'reviews_count': item['listing']['reviews_count'],
        # 'room_and_property_type': item['listing']['room_and_property_type'],
        # 'room_type': item['listing']['room_type'],
        # 'space_type': item['listing']['space_type'],
        # 'star_rating': item['listing']['star_rating'],
        # 'tier_id': item['listing']['tier_id'],
        # 'amenity_ids': item['listing']['amenity_ids'],
        # 'avg_rating': item['listing']['avg_rating'],
        # # pricing quote
        # 'can_instant_book': item['pricing_quote']['can_instant_book'],
        # 'monthly_price_factor': item['pricing_quote']['monthly_price_factor'],
        # 'amount': item['pricing_quote']['rate']['amount'],
        # 'rate_type': item['pricing_quote']['rate_type'],
        # # verified
        # 'enabled': item['verified']['enabled'],
        # # page
        # 'page': page

#     def add_image(self, current_message_id, file_id, user_id):
#         self.db.query(f"INSERT INTO images (message_id, image_id, author, post_time) "
#                                 f"values ({current_message_id} ,'{file_id}', {user_id}, '{dt.datetime.now().time()}')")
#         successfully_added = False
#         q = self.db.query(f"SELECT COUNT(*) FROM images WHERE message_id = {current_message_id}")[0]['count']
#         print("Q: " + str(q))
#         if q > 0:
#             successfully_added = True
#         else:
#             return False
#         print(f"Added image #{current_message_id}")
#         user_info = self.db.query(f"select no_posts from users where user_id = '{user_id}'")
#         posts = user_info[0]['no_posts']
#         posts += 1
#
#         self.db.query(f"UPDATE users SET no_posts = {posts} WHERE user_id = {user_id}")
#
#         return successfully_added
#
#         # self.add_user_points(author=user_id, points=1)
#
#     def update_image(self, current_message_id, user_id, user_choice):
#         result = True
#         print(f"Message id of image to update: {current_message_id}")
#         image = self.db.query(f"select author from images where message_id = '{current_message_id}'")
#
#         # Check if user already voted for this image, if so - dont add new likes/dislikes to author of the image
#         current_user_data = self.db.query(f"SELECT user_{user_id}, author FROM images WHERE message_id = {current_message_id} ")[0]
#         old_user_choice = current_user_data[f'user_{user_id}']
#
#         print(f"Old user choice: {old_user_choice} | New choice: {user_choice}")
#         if old_user_choice is None:
#             if user_id != current_user_data['author']:
#                 self.add_user_points(user_id)
#             self.add_user_stats(author=image[0]['author'], points=user_choice, old_choice=old_user_choice, first_time=True)
#         elif user_choice != old_user_choice:
#             self.add_user_stats(author=image[0]['author'], points=user_choice, old_choice=old_user_choice)
#         else:
#             result = False
#         self.db.query(f"UPDATE images SET user_{user_id} = {user_choice} where message_id = {current_message_id}")
#         return result
#
#         # Update images table
#
#         print("Updated message with new info")
#
#     def add_user_points(self, author):
#         print("Getting user points")
#         u_points = self.db.query(f"select user_points from users where user_id = '{author}'")[0]['user_points']
#         u_points += 1
#         if u_points < 0:
#             u_points = 0
#
#         self.db.query(f"UPDATE users SET user_points = {u_points} WHERE user_id = {author}")
#
#     def add_user_stats(self, author, points, old_choice, first_time=False):
#         print("Getting user points")
#         u_points = self.db.query(f"select user_points from users where user_id = '{author}'")[0]['user_points']
#
#         print("USER POINTS: " + str(u_points))
#         u_points += points
#         if u_points < 0:
#             u_points = 0
#
#         self.db.query(f"UPDATE users SET user_points = {u_points} WHERE user_id = {author}")
#         minus_likes = 0
#         minus_dislikes = 0
#         minus_what = 0
#
#         if not first_time:
#             if old_choice == 1:
#                 minus_likes = -1
#             elif old_choice == -1:
#                 minus_dislikes = -1
#             else:
#                 minus_what = -1
#
#         if points == 1:
#             self.add_likes_to_user(author, likes=1, dislikes=minus_dislikes, what=minus_what)
#         elif points == -1:
#             self.add_likes_to_user(author, likes=minus_likes, dislikes=1, what=minus_what)
#         else:
#             self.add_likes_to_user(author, likes=minus_likes, dislikes=minus_dislikes, what=1)
#
#     def get_image_stats(self, current_message_id):
#         image = self.db.query(f"select * from images where message_id = '{current_message_id}'")
#         likes = 0
#         dislikes = 0
#         what = 0
#         for ids in users_ids:
#             # if image[0]['author'] != f'user_{ids}':
#             if image[0][f'user_{ids}'] == 1:
#                 likes += 1
#             if image[0][f'user_{ids}'] == -1:
#                 dislikes += 1
#             if image[0][f'user_{ids}'] == 0:
#                 what += 1
#         return [likes, what, dislikes]
#
#     def get_users_stats(self):
#         data = []
#         users_info = self.db.query('SELECT * FROM users')
#         for user in users_info:
#             u_id = user['user_id']
#             u_rating = user['user_points']
#             u_rank = user['user_rank']
#             u_likes = user['likes']
#             u_what = user['what']
#             u_dislikes = user['dislikes']
#             u_no_posts = user['no_posts']
#             u_img_time = []
#             for i in range(0,24):
#                 u_images = self.db.query(f"SELECT COUNT(*) FROM images WHERE author = {user['user_id']} and post_time < '{i}:00:00' and post_time > '{max(0, i-1)}:00:00'")
#                 u_img_time.append(u_images[0]['count'])
#             u_most_popular_hour = u_img_time.index(max(u_img_time)) - 1
#             # print("mos_popular: " + str(u_most_popular_hour))
#             data.append({'id': u_id, 'rating': u_rating, 'rank': u_rank, 'likes': u_likes, 'what': u_what,
#                          'dislikes': u_dislikes, 'no_posts': u_no_posts, 'img_time': u_most_popular_hour})
#         # print(str(data))
#         return data
#
#     def add_likes_to_user(self, author, likes = 0, dislikes = 0, what = 0):
#         user_info = self.db.query(f"select likes, dislikes, what from users where user_id = '{author}'")
#         # print("Old user info: " + str(user_info[0]))
#         user_likes = user_info[0]['likes']
#         user_dislikes = user_info[0]['dislikes']
#         user_what = user_info[0]['what']
#         print(f"Old user likes: {user_likes} | {user_dislikes}")
#
#         user_likes += likes
#         user_dislikes += dislikes
#         user_what += what
#         if user_likes < 0:
#             user_likes = 0
#         if user_dislikes < 0:
#             user_dislikes = 0
#         if user_what < 0:
#             user_what = 0
#         print(f"New user likes: {user_likes} |{user_what} | {user_dislikes}")
#         self.db.query(f"UPDATE users SET likes = {user_likes}, dislikes = {user_dislikes}, "
#                       f"what = {user_what}  WHERE user_id = {author}")
#         # user_info = self.db.query(f"select * from users where user_id = '{author}'")
#
# # db1 = DBHelper()
# # db1.setup()
# # db.add_image(current_message_id=68, file_id="asdgas123454rdfaasd", user_id=421424722)
# # db.update_image(current_message_id=68, user_id=326997765, user_choice=1)
# # print(str(db.get_image_stats(68)))
# 'CREATE TABLE IF NOT EXISTS room (room_id INTEGER, page INTEGER, bathrooms  INTEGER, bed_label  VARCHAR(20, beds  INTEGER, bedroom_label  VARCHAR(80), ' \
#     'bedrooms  INTEGER, guest_label  INTEGER, is_business_travel_ready BOOLEAN, is_fully_refundable BOOLEAN, is_host_highly_rated BOOLEAN, ' \
#     'is_new_listing BOOLEAN, is_superhost BOOLEAN, lat DECIMAL(10,5), lng DECIMAL(10,5), localized_neighborhood VARCHAR(80), name VARCHAR(80), ' \
#     'neighborhood VARCHAR(80), person_capacity INTEGER, picture_count INTEGER, property_type_id INTEGER, reviews_count INTEGER, room_and_property_type VARCHAR(80), ' \
#     'room_type VARCHAR(80), space_type VARCHAR(80), star_rating DECIMAL(5,2), tier_id INTEGER, avg_rating DECIMAL(5,2)), can_insta_book BOOLEAN,' \
#     'monthly_price_factor DECIMAL(5,2), amount DECIMAL(5,2), rate_type VARCHAR(80), enabled BOOLEAN'
#
#     'CREATE TABLE amenity IF NOT EXISTS (room_id INTEGER, amentiy_id INTEGER, amenity VARCHAR(80), amenity_description VARCHAR(120))'

db = DBHelper()
db.list_rooms()