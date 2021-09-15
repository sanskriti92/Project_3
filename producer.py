from kafka import KafkaProducer
from datetime import datetime
import time
from json import dumps
import random
import string


topic = "project3"
producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda x: dumps(x).encode('utf-8'))


#-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
#21
customers = ["John Smith", "Nick Rimando", "Brad Davis", "Graham Zusi", "Julian Green", "Fabian Johnson", "Geoff Cameron", "Jozy Altidor", "Brad Guzan",\
        "Joe Biden", "Kane Viks", "Harsh Davidson", "Kayle Jamieson" , "Jeff Calvin" , "Ron Richards" , "Ross Geller" , "Charlie Decker" , "Jane Espinoza" , "Christin Labuchhane" , "Micahel Richards" , "Atul Gandhi"]
#-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
#64
electronic = ['TV', 'Mobile', 'Laptop', 'Camera', 'Printer', 'Headphones', 'Powerbank', 'SmartWatch', 'SDCard', 'Pendrive', 'HardDisk', 'RAM', 'Tablet', 'Projector', 'HomeTheatre']

clothing = ['T-shirt', 'Shirt', 'Jeans', 'Trouser', 'Track Pant', 'Shorts', 'SweatShirt', 'Jacket', 'Kurta', 'Saree']

footwear = ['Sports Shoes', 'Casual Shoes', 'Formal Shoes', 'Sandal', 'Floater', 'Flip-Flop', 'Boots', 'Running Shoes', 'Sneakers', 'Heels']

sports = ['Bat', 'Ball', 'Football', 'Stumps', 'Cricket Kit', 'Badminton', 'Shuttlecock']

home_furniture = ['Bedsheet', 'Curtain', 'Pillows', 'Blanket', 'Bath towel', 'Mattress', 'Wardrobe', 'Bed', 'Coffee Mug', 'Dinner Set', 'Wall Shelves', 'Water Bottle', 'Lunch Box']

stationary = ['Pen', 'Pencil', 'Notebooks', 'Diary', 'Calculator', 'Keychain', 'Stapler', 'Staple pins', 'Markers']

products = electronic + clothing + footwear + sports + home_furniture + stationary 
#---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
#8
payment_type_options = ["Card", "InternetBanking","Wallet","UPI","Phonepe","Gpay","Paytm","COD"]
#22
country_name_city_name_list = ["Sydney,Australia", "Florida,United States", "New York City,United States",
                                "Paris,France", "Colombo,Sri Lanka", "Dhaka,Bangladesh", "Islamabad,Pakistan",
                                "Beijing,China", "Rome,Italy", "Berlin,Germany", "Ottawa,Canada",
                                "London,United Kingdom", "Jerusalem,Israel", "Bangkok,Thailand",
                                "Chennai,India", "Bangalore,India", "Mumbai,India", "Pune,India",
                                "New Delhi,India", "Hyderabad,India", "Kolkata,India", "Singapore,Singapore"]
#8
ecommerce_website_name_list = ["www.datamaking.com", "www.amazon.in", "ShopClues.com", "www.flipkart.com", "www.snapdeal.com", "www.ebay.com", "www.ajio.com", "www.dmart.in"]
#10
failure_card_reasons = ["Invalid CVV", "Bad Network", "Invalid Card Number", "Invalid Otp", "Transaction_timed_out", "Insufficient Balance"]
failure_banking_reasons = ["Bad Network","Invalid Otp", "Transaction_timed_out", "Invalid username","Insufficient Balance"]
failure_wallet_reasons = ["Insufficient Balance", "Bad Network","Wallet not activated"]
failure_upi_reasons = ["Invalid UPI id", "Invalid PIN", "Transaction_timed_out", "Bad Network" ,"Insufficient Balance"]

#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

order_list = []
order = None
for i in range(100):
    order = {}
    print("Preparing order: " + str(i+1))
    event_datetime = datetime.now()

    order["order_id"] = i+1
    order["customer_id"] = random.randrange(3000,5000)
    order["customer_name"] = random.choice(customers)
    order["product_id"] = ''.join(random.sample(string.digits,6))
    order["product_name"] = random.choice(products)
    category = None
    if order["product_name"] in electronic:
        category = "Electronics"
    elif order["product_name"] in clothing:
        category = "Clothing"
    elif order["product_name"] in sports:
        category = "Sports"
    elif order["product_name"] in footwear:
        category = "Footwear"
    elif order["product_name"] in home_furniture:
        category = "Home & Furniture"
    elif order["product_name"] in stationary:
        category = "Stationary"

    order["product_category"] = category
    order["payment_type"] = random.choice(payment_type_options)
    order["qty"] = random.randint(1,10)
    order["price"] = round(random.uniform(1555.5, 7000.5), 2)
    order["datetime"] = event_datetime.strftime("%Y-%m-%d %H:%M:%S")

    country_name_city_name = random.choice(country_name_city_name_list)
    country_name = country_name_city_name.split(",")[1]
    city_name = country_name_city_name.split(",")[0]

    order["country"] = country_name
    order["city"] = city_name
    order["ecommerce_website_name"] = random.choice(ecommerce_website_name_list)
    order["payment_txn_id"] = ''.join(random.sample(string.digits+string.ascii_letters,12))
    order["payment_txn_success"] = random.choice(['Y','N'])
    
    if order["payment_txn_success"] == 'N':
        if order["payment_type"] == 'Card':
            failure_reason = random.choice(failure_card_reasons)
        elif order["payment_type"] == 'InternetBanking':
            failure_reason = random.choice(failure_banking_reasons)
        elif order["payment_type"] == 'Wallet':
            failure_reason = random.choice(failure_wallet_reasons)
        else:
            failure_reason = random.choice(failure_upi_reasons)
        order["failure_reason"] = failure_reason

    else:
        order["failure_reason"] = ""

    print("Order: ", order)
    producer.send(topic, order)
    time.sleep(2)

    print("Kafka Producer Application Completed. ")



# order_id	Order Id
# customer_id	Customer Id
# customer_name	Customer Name
# product_id	Product Id
# product_name	Product Name
# product_category	Product Category
# payment_type	Payment Type (card, Internet Banking, UPI, Wallet)
# qty	Quantity ordered
# price	Price of the product
# datetime	Date and time when order was placed
# country	Customer Country
# city	Customer City
# ecommerce_website_name	Site from where order was placed
# payment_txn_id	Payment Transaction Confirmation Id
# payment_txn_success	Payment Success or Failure (Y=Success. N=Failed)
# failure_reason	Reason for payment failure
