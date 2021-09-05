from kafka import KafkaProducer
from datetime import datetime
import time
from json import dumps
import random
import string


topic = "Project3"
server_port = 'localhost:9092'

if __name__ == "__main__":

    producer = KafkaProducer(bootstrap_servers=server_port,
                                       value_serializer=lambda x: dumps(x).encode('utf-8'))


#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

    customers = {"John Smith" : 3001, "Nick Rimando": 3002,"Brad Davis": 3003,"Graham Zusi":3004,"Julian Green": 3005,"Fabian Johnson": 3006,\
            "Geoff Cameron": 3006,"Jozy Altidor": 3007,"Brad Guzan": 3008, "Joe Biden": 3009, "Kane Viks":3010,"Harsh Davidson":3011,"Kayle Jamieson" :3012,\
            "Jeff Calvin":3013,"Ron Richards":3014,"Ross Geller":3015,"Charlie Decker":3016, "Jane Espinoza":3017, "Christin Labuchhane":3018, "Micahel Richards":3019, "Atul Gandhi":3020}

#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    electronic = {'TV':101,'Mobile':102, 'Laptop':103, 'Camera':104, 'Printer':105, 'Headphones':106, \
        'Powerbank':107,'SmartWatch':108,'SDCard':109,'Pendrive':110,"HardDisk":111,'RAM':112,'Tablet':113,'Projector':114,'HomeTheatre':115,'Router':116}
    clothing = {'T-shirt':201,'Shirt':202,'Jeans':203,'Trouser':204,'Track Pant':205,'Shorts':206,'SweatShirt':207,'Jacket':208,'Kurta':209,'Saree':210} 
    footwear = {'Sports Shoes':301,'Casual Shoes':302,'Formal Shoes':303,'Sandal':304,'Floater':305,'Flip-Flop':306,'Boots':307,'Running Shoes':308,'Sneakers':309,'Heels':310}
    sports = {'Bat':401,'Ball':402, 'Football':403,'Stumps':404,'Cricket Kit':405,'Badminton':406,'Shuttlecock':407}
    home_furniture = {'Bedsheet':501,'Curtain':502,'Cushion':503,'Pillows':503,'Blanket':504,'Bath towel':505,'Mattress':506,'Wardrobe':507,'Bed':508,'Coffee Mug':509,\
                    'Dinner Set':510, 'Wall Shelves':511, 'Water Bottle':512, 'Lunch Box':513}
    stationary = {'Pen':601,'Pencil':602,'Notebooks':603,'Diary':604,'Calculator':605,'Keychain':606,'Stapler':607,'Staple pins':608,'Markers':609}

    products = {**electronic,**clothing,**footwear,**sports,**home_furniture,**stationary}
#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    payment_type_options = ["Card", "InternetBanking","Wallet","UPI","Gpay","PhonePe","Paytm"]

    country_name_city_name_list = ["Sydney,Australia", "Florida,United States", "New York City,United States",
                                   "Paris,France", "Colombo,Sri Lanka", "Dhaka,Bangladesh", "Islamabad,Pakistan",
                                   "Beijing,China", "Rome,Italy", "Berlin,Germany", "Ottawa,Canada",
                                   "London,United Kingdom", "Jerusalem,Israel", "Bangkok,Thailand",
                                   "Chennai,India", "Bangalore,India", "Mumbai,India", "Pune,India",
                                   "New Delhi,India", "Hyderabad,India", "Kolkata,India", "Singapore,Singapore"]

    ecommerce_website_name_list = ["www.datamaking.com", "www.amazon.com", "www.flipkart.com", "www.snapdeal.com", "www.ebay.com"]

    failure_card_reasons = ["Invalid CVV", "Bad Network", "Invalid Card Number", "Invalid Otp", "Transaction_timed_out","Insufficient Balance"]
    failure_banking_reasons = ["Bad Network","Invalid Otp", "Transaction_timed_out", "Invalid username","Insufficient Balance"]
    failure_wallet_reasons = ["Insufficient Balance","Bad Network","Wallet not activated"]
    failure_upi_reasons = ["Invalid UPI id", "Invalid PIN", "Transaction_timed_out", "Bad Network" ,"Insufficient Balance"]

    order_list = []
    order = None
    for i in range(1,50):
        order = {}
        print("Preparing message: " + str(i))
        event_datetime = datetime.now()

        order["order_id"] = i
        customer_name = random.choice(list(customers))
        order["customer_id"] = customers[customer_name]
        order["customer_name"] = customer_name
        product_name = random.choice(list(products))
        order["product_id"] = products[product_name]
        order["product_name"] = product_name
        category = None
        if product_name in electronic:
            category = "Electronics"
        elif product_name in clothing:
            category = "Clothing"
        elif product_name in sports:
            category = "Sports"
        elif product_name in footwear:
            category = "Footwear"
        elif product_name in home_furniture:
            category = "Home & Furniture"
        elif product_name in stationary:
            category = "Stationary"

        order["product_category"] = category
        order["payment_type"] = random.choice(payment_type_options)
        order["qty"] = random.randint(1,10)
        order["price"] = round(random.uniform(5.5, 1550.5), 2)
        order["datetime"] = event_datetime.strftime("%Y-%m-%d %H:%M:%S")
        country_name = None
        city_name = None
        country_name_city_name = None
        country_name_city_name = random.choice(country_name_city_name_list)
        country_name = country_name_city_name.split(",")[1]
        city_name = country_name_city_name.split(",")[0]
        order["country"] = country_name
        order["city"] = city_name
        order["ecommerce_website_name"] = random.choice(ecommerce_website_name_list)
        length = 12
        order["payment_txn_id"] = ''.join(random.sample(string.digits+string.ascii_letters,length))
        order["payment_txn_success"] = random.choice(['Y','N'])

        if order["payment_type"] == 'Card':
            failure_reason = random.choice(failure_card_reasons)
        elif order["payment_type"] == 'InternetBanking':
            failure_reason = random.choice(failure_banking_reasons)
        elif order["payment_type"] == 'Wallet':
            failure_reason = random.choice(failure_wallet_reasons)
        else:
            failure_reason = random.choice(failure_upi_reasons)
        
        if order["payment_txn_success"] == 'N':
            order["failure_reason"] = failure_reason
        else:
            order["failure_reason"] = ""

        print("Order: ", order)
        order_list.append(order)
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
