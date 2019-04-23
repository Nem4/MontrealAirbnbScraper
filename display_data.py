import folium
from db import DBHelper
import colorsys
import sys
import linecache
import matplotlib.pyplot as plt
import matplotlib

font = {'size': 36}

matplotlib.rc('font', **font)
db = DBHelper()
class MapGenerator:
    def generate_map(self):
        m = folium.Map(location=[45.509484, -73.600519], tiles="Stamen Terrain", zoom_start=10.5)

        db = DBHelper()
        max_price = 300
        coordinates = db.get_coordinates(max_price=max_price)


        def hsv2rgb(h,s,v):
            return tuple(round(i * 255) for i in colorsys.hsv_to_rgb(h,s,v))



        def print_exception():
            exc_type, exc_obj, tb = sys.exc_info()
            f = tb.tb_frame
            lineno = tb.tb_lineno
            filename = f.f_code.co_filename
            linecache.checkcache(filename)
            line = linecache.getline(filename, lineno, f.f_globals)
            print('EXCEPTION IN ({}, LINE {} "{}"): {}'.format(filename, lineno, line.strip(), exc_obj))
            print(str(sys.exc_info()))
        print(f"points number: {len(coordinates)}")
        # I can add marker one by one on the map
        for coordinate in coordinates:
            amount_n = float((max_price - coordinate['amount'])/max_price)
            test_color = hsv2rgb(0.8, amount_n, 1 - amount_n)
            try:
                folium.Circle(
                    location=[coordinate['lat'], coordinate['lng']],
                    popup=f"${coordinate['amount']} : https://www.airbnb.ca/rooms/{coordinate['room_id']}",
                    radius=10,
                    color='#%02x%02x%02x' % test_color,
                    fill=True
                    # fill_color='crimson'
                ).add_to(m)
            except:
                print(f"color: {test_color}")
                print_exception()

        # Save it as html
        m.save('mymap.html')

class GrafsGenerator:

    def __init__(self):
        self.max_price = 300

    def generate_vertical_price_histogram(self):
        # Import library and dataset
        import matplotlib.pyplot as plt
        x = db.get_avg_price_per_page(max_price=self.max_price)
        plt.figure(figsize=(50, 10))
        plt.title('AVG PRICE PER PAGE')
        plt.ylabel('Probability')
        y = []
        for i in range(0, 17):
            y.append(f"{i}")
        plt.bar(y, height=x, width=0.8, align='center')
        for i, v in enumerate(x):
            plt.text(i - .1, v / x[i] + 10, round(x[i]), fontsize=24, color="white")

        plt.show()

    def generate_vertical_price_bed_histogram(self):
        # Import library and dataset

        x = db.get_avg_price_bed_per_page(max_price=self.max_price)
        plt.figure(figsize=(50, 10))
        plt.title('PRICE$ per BED')
        plt.ylabel('Probability')
        y = []
        for i in range(0, 17):
            y.append(f"{i}")
        plt.bar(y, height=x, width=0.8, align='center')
        for i, v in enumerate(x):
            plt.text(i - .1, v / x[i] + 10, round(x[i]), fontsize=24, color="white")
        plt.show()

    def generate_vertical_avg_guests_histogram(self):
        # Import library and dataset
        x = db.get_guset_label_per_page(max_price=self.max_price)
        # print(str(x))
        plt.figure(figsize=(50, 10))
        plt.title('AVG GUESTS PER PAGE')
        plt.ylabel('Probability')
        y = []
        for i in range(0, 17):
            y.append(f"{i}")
        plt.bar(y, height=x, width=0.8, align='center')
        for i, v in enumerate(x):
            plt.text(i - .1, v / x[i] + 2, round(x[i], 2), fontsize=24, color="white")

        plt.show()



map = MapGenerator()
map.generate_map()
# grafs = GrafsGenerator()
# grafs.generate_vertical_price_histogram()
# grafs.generate_vertical_price_bed_histogram()
# grafs.generate_vertical_avg_guests_histogram()