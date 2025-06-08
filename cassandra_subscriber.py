import pika
import json
import time
import os
from dotenv import load_dotenv
from datetime import datetime
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import BatchStatement, SimpleStatement, BatchType

# Load environment variables
load_dotenv()

# --- Configuracion de Cassandra ---
CASSANDRA_HOSTS = os.getenv('CASSANDRA_HOSTS', '127.0.0.1').split(',')
CASSANDRA_KEYSPACE = os.getenv('CASSANDRA_KEYSPACE', 'realtime_analytics_ks')
CASSANDRA_USERNAME = os.getenv('CASSANDRA_USERNAME')
CASSANDRA_PASSWORD = os.getenv('CASSANDRA_PASSWORD')

# --- Datos de Geografia (del publicador) ---
GEOGRAPHY_DATA = [
    {
        "GeographyKey": 655, "City": "Rock Springs", "StateProvinceCode": "WY",
        "StateProvinceName": "Wyoming", "CountryRegionCode": "US",
        "EnglishCountryRegionName": "United States", "SpanishCountryRegionName": "Estados Unidos",
        "FrenchCountryRegionName": "États-Unis", "PostalCode": "82901",
        "SalesTerritoryKey": 1, "IpAddressLocator": "203.0.113.148"
    },
    {
        "GeographyKey": 654, "City": "Cheyenne", "StateProvinceCode": "WY",
        "StateProvinceName": "Wyoming", "CountryRegionName": "US",
        "EnglishCountryRegionName": "United States", "SpanishCountryRegionName": "Estados Unidos",
        "FrenchCountryRegionName": "États-Unis", "PostalCode": "82001",
        "SalesTerritoryKey": 1, "IpAddressLocator": "203.0.113.147"
    },
    {
        "GeographyKey": 589, "City": "Plano", "StateProvinceCode": "TX",
        "StateProvinceName": "Texas", "CountryRegionCode": "US",
        "EnglishCountryRegionName": "United States", "SpanishCountryRegionName": "Estados Unidos",
        "FrenchCountryRegionName": "États-Unis", "PostalCode": "75074",
        "SalesTerritoryKey": 4, "IpAddressLocator": "203.0.113.82"
    },
    {
        "GeographyKey": 483, "City": "Jefferson City", "StateProvinceCode": "MO",
        "StateProvinceName": "Missouri", "CountryRegionCode": "US",
        "EnglishCountryRegionName": "United States", "SpanishCountryRegionName": "Estados Unidos",
        "FrenchCountryRegionName": "États-Unis", "PostalCode": "65101",
        "SalesTerritoryKey": 3, "IpAddressLocator": "192.0.2.230"
    }
]

# Dictionary for quick geography lookup
GEOGRAPHY_MAP = {data["GeographyKey"]: data for data in GEOGRAPHY_DATA}

# --- Cassandra Connection (global for reuse) ---
cluster = None
session = None

def get_cassandra_session():
    """Establishes and returns the Cassandra session."""
    global cluster, session
    if session is None or session.is_shutdown:
        try:
            auth_provider = PlainTextAuthProvider(
                username=CASSANDRA_USERNAME,
                password=CASSANDRA_PASSWORD
            ) if CASSANDRA_USERNAME and CASSANDRA_PASSWORD else None

            cluster = Cluster(
                CASSANDRA_HOSTS,
                auth_provider=auth_provider
            )
            session = cluster.connect(CASSANDRA_KEYSPACE)
            print(f"✅ Connected to Cassandra at {CASSANDRA_HOSTS[0]}, Keyspace: {CASSANDRA_KEYSPACE}")
        except Exception as e:
            print(f"❌ Error connecting to Cassandra: {e}")
            session = None
    return session

# --- Helper functions to prepare data ---
def get_country_name_from_geography(geography_key):
    """Gets the country name from GeographyKey."""
    geo_info = GEOGRAPHY_MAP.get(geography_key)
    return geo_info["EnglishCountryRegionName"] if geo_info else "Unknown"

def get_time_buckets(timestamp_seconds):
    """Generates time buckets for counts (hour, day, 5min)."""
    dt_object = datetime.fromtimestamp(timestamp_seconds)
    
    hour_bucket = dt_object.strftime('%Y%m%d%H')
    daily_bucket = dt_object.strftime('%Y%m%d')
    minute_rounded = (dt_object.minute // 5) * 5
    five_min_bucket = dt_object.replace(minute=minute_rounded, second=0, microsecond=0).strftime('%Y%m%d%H%M')
    
    return {
        'hourly': f'hourly:{hour_bucket}',
        'daily': f'daily:{daily_bucket}',
        '5min': f'5min:{five_min_bucket}'
    }

# --- Main callback to process RabbitMQ messages ---
def callback(ch, method, properties, body):
    mensaje = json.loads(body)
    print(f"📥 Received message: {mensaje.get('type')} - {properties.message_id}")

    session = get_cassandra_session()
    if not session:
        print("❌ No Cassandra connection. Requeuing message.")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
        return

    try:
        message_timestamp = datetime.fromtimestamp(properties.timestamp) 

        if mensaje.get('type') == 'customer':
            customer_alt_key = mensaje['CustomerAlternateKey']
            city = mensaje['City']
            country_name = get_country_name_from_geography(mensaje['GeographyKey'])

            # Batch para operaciones NO-CONTADOR (INSERT/UPDATE)
            non_counter_batch = BatchStatement()

            # 1. customer_latest_info
            insert_customer_cql = session.prepare("""
                INSERT INTO customer_latest_info (
                    customer_alternate_key, registration_timestamp, first_name, last_name, email_address, phone, address_line1,
                    address_line2, city, state_province_code, postal_code, title, middle_name, name_style, birth_date,
                    marital_status, suffix, gender, yearly_income, total_children, number_children_at_home,
                    english_education, spanish_education, french_education, english_occupation, spanish_occupation,
                    french_occupation, house_owner_flag, number_cars_owned, date_first_purchase, commute_distance
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """)
            non_counter_batch.add(insert_customer_cql, (
                customer_alt_key, message_timestamp, mensaje['FirstName'], mensaje['LastName'], mensaje['EmailAddress'],
                mensaje['Phone'], mensaje['AddressLine1'], mensaje['AddressLine2'], mensaje['City'], mensaje['StateProvinceCode'],
                mensaje['PostalCode'], mensaje['Title'], mensaje['MiddleName'], mensaje['NameStyle'],
                datetime.strptime(mensaje['BirthDate'], '%Y-%m-%d').date(),
                mensaje['MaritalStatus'], mensaje['Suffix'], mensaje['Gender'], mensaje['YearlyIncome'],
                mensaje['TotalChildren'], mensaje['NumberChildrenAtHome'], mensaje['EnglishEducation'],
                mensaje['SpanishEducation'], mensaje['FrenchEducation'], mensaje['EnglishOccupation'],
                mensaje['SpanishOccupation'], mensaje['FrenchOccupation'], mensaje['HouseOwnerFlag'],
                mensaje['NumberCarsOwned'], datetime.strptime(mensaje['DateFirstPurchase'], '%Y-%m-%d').date(),
                mensaje['CommuteDistance']
            ))

            insert_global_recent_cql = session.prepare("""
                INSERT INTO global_recent_customers (
                    fixed_partition_key, registration_timestamp, customer_alternate_key, first_name, last_name, email_address, city, date_first_purchase
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """)
            non_counter_batch.add(insert_global_recent_cql, (
                'all_customers', # La clave de partición fija
                message_timestamp,
                customer_alt_key,
                mensaje['FirstName'],
                mensaje['LastName'],
                mensaje['EmailAddress'],
                mensaje['City'],
                datetime.strptime(mensaje['DateFirstPurchase'], '%Y-%m-%d').date()
            ))

            # Ejecutar batch de no-contadores
            session.execute(non_counter_batch)
            print(f"📝 Non-counter batch para cliente {customer_alt_key} ejecutado.")

            # Batch para operaciones de CONTADOR - UNLOGGED
            counter_batch = BatchStatement(batch_type=BatchType.UNLOGGED)
            
            # 3. new_customer_geo_counts_by_hour (contador)
            hour_bucket = get_time_buckets(properties.timestamp)['hourly'].split(':')[1]
            update_geo_count_cql = session.prepare("""
                UPDATE new_customer_geo_counts_by_hour
                SET new_customers_count = new_customers_count + 1
                WHERE hour_bucket = ? AND country_region_name = ? AND city = ?;
            """)
            counter_batch.add(update_geo_count_cql, (hour_bucket, country_name, city))

            # Ejecutar batch de contadores
            session.execute(counter_batch)
            print(f"📝 Counter batch para cliente {customer_alt_key} ejecutado.")

            print(f"✅ Cliente {customer_alt_key} guardado en Cassandra.")

        elif mensaje.get('type') == 'product':
            product_alt_key = mensaje['ProductAlternateKey']
            product_subcategory_key = mensaje['ProductSubcategoryKey']
            english_product_name = mensaje['EnglishProductName']
            color = mensaje['Color']

            # Batch para operaciones NO-CONTADOR (INSERT/UPDATE)
            non_counter_batch = BatchStatement()

            # 1. latest_product_category_trends (for "category of most recently added products")
            insert_category_trends_cql = session.prepare("""
                INSERT INTO latest_product_category_trends (
                    product_subcategory_key, addition_timestamp, product_alternate_key, english_product_name, color
                ) VALUES (?, ?, ?, ?, ?)
            """)
            non_counter_batch.add(insert_category_trends_cql, (
                product_subcategory_key, message_timestamp, product_alt_key, english_product_name, color
            ))
            
            # Ejecutar batch de no-contadores
            session.execute(non_counter_batch)
            print(f"📝 Non-counter batch para producto {product_alt_key} ejecutado.")

            # Batch para operaciones de CONTADOR - UNLOGGED
            counter_batch = BatchStatement(batch_type=BatchType.UNLOGGED)

            # 2. new_products_total_count_by_time (counters for different buckets)
            time_buckets = get_time_buckets(properties.timestamp)
            update_product_count_cql = session.prepare("""
                UPDATE new_products_total_count_by_time
                SET product_count = product_count + 1
                WHERE time_bucket = ?;
            """)
            for bucket_type, bucket_value in time_buckets.items():
                counter_batch.add(update_product_count_cql, (bucket_value,))

            # Ejecutar batch de contadores
            session.execute(counter_batch)
            print(f"📝 Counter batch para producto {product_alt_key} ejecutado.")

            print(f"✅ Producto {product_alt_key} guardado en Cassandra.")

        ch.basic_ack(delivery_tag=method.delivery_tag)
        print("✅ Message acknowledged (ACK)")

    except Exception as e:
        print(f"❌ Error processing message or inserting into Cassandra: {e}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
        if session and session.is_shutdown:
            print("Cassandra connection lost. Attempting to reconnect in the next cycle.")


# --- Function to start the RabbitMQ subscriber ---
def start_subscriber():
    retry_delay = 5  # segundos entre intentos de reconexion
    rabbitmq_host = os.getenv('RABBITMQ_HOST', 'localhost')

    while True:
        connection = None
        channel = None
        try:
            # Establecer conexión con RabbitMQ
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(
                    host=rabbitmq_host,
                    heartbeat=600,
                    blocked_connection_timeout=300
                )
            )
            channel = connection.channel()

            exchange_name = 'mensajes_fanout_durable'
            channel.exchange_declare(exchange=exchange_name, exchange_type='fanout', durable=True)

            # Crear cola duradera con nombre especifico
            queue_name = 'cassandra_subscriber_queue_durable'
              # Intentar declarar la cola sin argumentos primero
            try:
                result = channel.queue_declare(queue=queue_name, durable=True)
            except pika.exceptions.ChannelClosedByBroker:
                # Si falla, reconectar y declarar con los argumentos
                connection = pika.BlockingConnection(
                    pika.ConnectionParameters(host=rabbitmq_host)
                )
                channel = connection.channel()
                result = channel.queue_declare(
                    queue=queue_name, 
                    durable=True
                )
            
            # Incluir el binding entre la cola y el exchange
            channel.queue_bind(
                exchange=exchange_name,
                queue=queue_name
            )
            
            # Configurar QoS mas conservador
            channel.basic_qos(prefetch_count=1)

            channel.basic_consume(queue=queue_name, on_message_callback=callback, auto_ack=False)
            print(f"👂 Cassandra Subscriber listening on exchange '{exchange_name}' and queue '{queue_name}'")
            print(f"📊 Messages in queue: {result.method.message_count}")
            
            channel.start_consuming()
            
        except (pika.exceptions.ConnectionClosedByBroker, pika.exceptions.AMQPChannelError) as e:
            print(f"🔄 Connection error: {str(e)}, retrying in {retry_delay} seconds...")
            time.sleep(retry_delay)
            continue
        except KeyboardInterrupt:
            print("🛑 Stopping Cassandra subscriber...")
            if channel:
                try:
                    channel.close()
                except Exception:
                    pass
            if connection:
                try:
                    connection.close()
                except Exception:
                    pass
            if cluster:
                try:
                    cluster.shutdown()
                except Exception:
                    pass
            break
        except Exception as e:
            print(f"❌ Unexpected error: {str(e)}")
            if channel:
                try:
                    channel.close()
                except Exception:
                    pass
            if connection:
                try:
                    connection.close()
                except Exception:
                    pass
            if cluster:
                try:
                    cluster.shutdown()
                except Exception:
                    pass
            time.sleep(retry_delay)
            continue

if __name__ == "__main__":
    start_subscriber()
