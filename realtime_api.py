from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from datetime import datetime, timedelta, date # Import date specifically
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# --- Configuracion de Flask ---
app = Flask(__name__, static_folder='static')
CORS(app) 

# Ruta para servir el dashboard
@app.route('/')
def serve_dashboard():
    return send_from_directory(app.static_folder, 'index.html')

# --- Configuracion de Cassandra ---
CASSANDRA_HOSTS = os.getenv('CASSANDRA_HOSTS').split(',') if os.getenv('CASSANDRA_HOSTS') else None
CASSANDRA_KEYSPACE = os.getenv('CASSANDRA_KEYSPACE')
CASSANDRA_USERNAME = os.getenv('CASSANDRA_USERNAME')
CASSANDRA_PASSWORD = os.getenv('CASSANDRA_PASSWORD')

# --- Conexion a Cassandra (global para reutilizar) ---
cluster = None
session = None

def get_cassandra_session():
    """Establece y devuelve la sesion de Cassandra."""
    global cluster, session
    if session is None or session.is_shutdown:
        try:
            if not CASSANDRA_HOSTS or not CASSANDRA_KEYSPACE:
                print("❌ Error: CASSANDRA_HOSTS y CASSANDRA_KEYSPACE deben estar configurados en el archivo .env")
                return None

            auth_provider = None
            if CASSANDRA_USERNAME and CASSANDRA_PASSWORD:
                auth_provider = PlainTextAuthProvider(
                    username=CASSANDRA_USERNAME,
                    password=CASSANDRA_PASSWORD
                )
            
            cluster = Cluster(
                CASSANDRA_HOSTS,
                auth_provider=auth_provider if auth_provider else None
            )
            session = cluster.connect(CASSANDRA_KEYSPACE)
            print(f"✅ API conectada a Cassandra en {CASSANDRA_HOSTS[0]}, Keyspace: {CASSANDRA_KEYSPACE}")
        except Exception as e:
            print(f"❌ Error al conectar la API a Cassandra: {e}")
            session = None
    return session

# --- Mapeo de Subcategorias de Producto (ejemplo, ajusta segun los datos de tu publicador) ---
PRODUCT_SUBCATEGORIES_MAP = {
    1: "Mountain Bikes",
    2: "Road Bikes", # El publicador genera ProductSubcategoryKey 2
    3: "Touring Bikes",
}

# --- Funciones Auxiliares para Generar Buckets de Tiempo ---
def get_current_hour_bucket():
    """Retorna el bucket horario actual (YYYYMMDDHH)."""
    return datetime.now().strftime('%Y%m%d%H')

def get_current_5min_bucket():
    """Retorna el bucket de 5 minutos actual (YYYYMMDDHHMM_5min)."""
    now = datetime.now()
    minute_rounded = (now.minute // 5) * 5
    return now.replace(minute=minute_rounded, second=0, microsecond=0).strftime('%Y%m%d%H%M')

def get_current_daily_bucket():
    """Retorna el bucket diario actual (YYYYMMDD)."""
    return datetime.now().strftime('%Y%m%d')

# --- Endpoints de la API ---

@app.route('/api/v1/status', methods=['GET'])
def api_status():
    """Endpoint para verificar el estado de la API y la conexion a Cassandra."""
    session = get_cassandra_session()
    if session:
        return jsonify({"status": "API is running", "cassandra_connected": True}), 200
    else:
        return jsonify({"status": "API is running", "cassandra_connected": False, "message": "Could not connect to Cassandra"}), 500

# 1.1 Obtener datos del ultimo cliente registrado por ID
@app.route('/api/v1/customers/latest_info/<string:customer_alternate_key>', methods=['GET'])
def get_latest_customer_info(customer_alternate_key):
    session = get_cassandra_session()
    if not session:
        return jsonify({"error": "Cassandra connection failed"}), 500
    
    try:
        query = session.prepare("SELECT * FROM customer_latest_info WHERE customer_alternate_key = ?")
        row = session.execute(query, [customer_alternate_key]).one()
        
        if row:
            row_dict = row._asdict() 
            # Convertir objetos date y timestamp a strings para JSON de forma robusta
            if row_dict.get('birth_date'):
                try:
                    row_dict['birth_date'] = row_dict['birth_date'].isoformat()
                except AttributeError:
                    row_dict['birth_date'] = str(row_dict['birth_date']) # Fallback if not a date object with isoformat
            if row_dict.get('date_first_purchase'):
                try:
                    row_dict['date_first_purchase'] = row_dict['date_first_purchase'].isoformat()
                except AttributeError:
                    row_dict['date_first_purchase'] = str(row_dict['date_first_purchase']) # Fallback
            if row_dict.get('registration_timestamp'):
                try:
                    row_dict['registration_timestamp'] = row_dict['registration_timestamp'].isoformat()
                except AttributeError:
                    row_dict['registration_timestamp'] = str(row_dict['registration_timestamp']) # Fallback

            return jsonify(row_dict), 200
        else:
            return jsonify({"message": "Customer not found"}), 404
    except Exception as e:
        print(f"Error al consultar customer_latest_info: {e}")
        return jsonify({"error": f"Error al consultar cliente: {str(e)}"}), 500

# Nuevo Endpoint para obtener los N clientes mas recientes a nivel global (usando la nueva tabla de buenas practicas)
@app.route('/api/v1/customers/global_recent', methods=['GET'])
def get_global_recent_customers():
    session = get_cassandra_session()
    if not session:
        return jsonify({"error": "Cassandra connection failed"}), 500
    
    # Obtener el numero de clientes a devolver (default a 10)
    limit = request.args.get('limit', 10, type=int)
    if limit <= 0:
        return jsonify({"error": "El parametro 'limit' debe ser un numero positivo."}), 400

    try:
        # Consulta la tabla global_recent_customers, que ya esta optimizada para esta consulta
        # debido a su modelado de datos con fixed_partition_key y CLUSTERING ORDER BY (registration_timestamp DESC)
        query = session.prepare("""
            SELECT customer_alternate_key, first_name, last_name, email_address, city, registration_timestamp, date_first_purchase
            FROM global_recent_customers
            WHERE fixed_partition_key = 'all_customers'
            LIMIT ?;
        """)
        rows = session.execute(query, [limit])
        
        results = []
        for row in rows:
            # FIX: Convert Cassandra Row object to a dictionary using _asdict() first
            row_dict = row._asdict()

            # Convertir objetos date y timestamp a strings para JSON de forma robusta
            formatted_date_first_purchase = None
            if row_dict.get('date_first_purchase'):
                try:
                    formatted_date_first_purchase = row_dict['date_first_purchase'].isoformat()
                except AttributeError:
                    formatted_date_first_purchase = str(row_dict['date_first_purchase']) # Fallback
            
            formatted_registration_timestamp = None
            if row_dict.get('registration_timestamp'):
                try:
                    formatted_registration_timestamp = row_dict['registration_timestamp'].isoformat()
                except AttributeError:
                    formatted_registration_timestamp = str(row_dict['registration_timestamp']) # Fallback

            results.append({
                "customer_alternate_key": row_dict['customer_alternate_key'],
                "first_name": row_dict['first_name'],
                "last_name": row_dict['last_name'],
                "email_address": row_dict['email_address'],
                "city": row_dict['city'],
                "date_first_purchase": formatted_date_first_purchase,
                "registration_timestamp": formatted_registration_timestamp
            })
        
        if not results:
            return jsonify({"message": "No global recent customers found"}), 404
        
        return jsonify({"global_recent_customers": results}), 200
    except Exception as e:
        print(f"Error al consultar global_recent_customers: {e}")
        return jsonify({"error": f"Error al obtener los clientes globales recientes: {str(e)}"}), 500




# 2. Cual es la distribucion geografica (ciudad/pais) de los clientes nuevos en este momento?
# La consulta ahora recupera la distribucion por ciudad para un pais especifico en la hora actual.
@app.route('/api/v1/customers/geo_distribution_hourly_by_country/<string:country_name>', methods=['GET'])
def get_new_customer_geo_distribution_hourly_by_country(country_name):
    if not country_name:
        return jsonify({"error": "Se requiere el nombre del país"}), 400
        
    session = get_cassandra_session()
    if not session:
        return jsonify({"error": "Cassandra connection failed"}), 500
    
    try:
        current_hour_bucket = get_current_hour_bucket()
        # La consulta ahora utiliza toda la clave de particion (hour_bucket, country_region_name)
        # lo que la hace eficiente y no requiere ALLOW FILTERING.
        query = session.prepare("SELECT country_region_name, city, new_customers_count FROM new_customer_geo_counts_by_hour WHERE hour_bucket = ? AND country_region_name = ?")
        rows = session.execute(query, [current_hour_bucket, country_name])
        
        results = []
        for row in rows:
            results.append({
                "country_region_name": row.country_region_name,
                "city": row.city,
                "new_customers_count": row.new_customers_count
            })
        
        total_new_customers = sum(item['new_customers_count'] for item in results)

        return jsonify({
            "hour_bucket": current_hour_bucket,
            "country_name": country_name,
            "total_new_customers_in_hour_for_country": total_new_customers,
            "distribution_by_city": results
        }), 200
    except Exception as e:
        print(f"Error al consultar new_customer_geo_counts_by_hour: {e}")
        return jsonify({"error": f"Error al consultar distribucion geografica por pais: {str(e)}"}), 500

# 3. Cuantos productos nuevos se han anadido al catalogo en la ultima hora/dia/5 minutos?
@app.route('/api/v1/products/new_count', methods=['GET'])
def get_new_products_count():
    session = get_cassandra_session()
    if not session:
        return jsonify({"error": "Cassandra connection failed"}), 500
    
    period = request.args.get('period', 'hourly') # Default a 'hourly'
    
    try:
        time_bucket_prefix = ""
        bucket_value = ""

        if period == 'hourly':
            time_bucket_prefix = 'hourly:'
            bucket_value = get_current_hour_bucket()
        elif period == 'daily':
            time_bucket_prefix = 'daily:'
            bucket_value = get_current_daily_bucket()
        elif period == '5min':
            time_bucket_prefix = '5min:'
            bucket_value = get_current_5min_bucket()
        else:
            return jsonify({"error": "Parametro 'period' invalido. Use 'hourly', 'daily' o '5min'."}), 400

        full_time_bucket = time_bucket_prefix + bucket_value
        
        query = session.prepare("SELECT product_count FROM new_products_total_count_by_time WHERE time_bucket = ?")
        row = session.execute(query, [full_time_bucket]).one()
        
        count = row.product_count if row else 0
        
        return jsonify({
            "period": period,
            "time_bucket": bucket_value,
            "new_products_count": count
        }), 200
    except Exception as e:
        print(f"Error al consultar new_products_total_count_by_time: {e}")
        return jsonify({"error": f"Error al consultar conteo de productos: {str(e)}"}), 500

# 4. Cual es la categoria de los productos mas recientemente anadidos?
@app.route('/api/v1/products/recent_by_category/<int:product_subcategory_key>', methods=['GET'])
def get_recent_products_by_category(product_subcategory_key):
    if product_subcategory_key not in PRODUCT_SUBCATEGORIES_MAP:
        return jsonify({"error": "Categoría de producto no válida"}), 400
        
    session = get_cassandra_session()
    if not session:
        return jsonify({"error": "Cassandra connection failed"}), 500
    
    try:
        query = session.prepare("""
            SELECT product_alternate_key, english_product_name, color, addition_timestamp
            FROM latest_product_category_trends
            WHERE product_subcategory_key = ?
            LIMIT 10;
        """)
        rows = session.execute(query, [product_subcategory_key])
        
        results = []
        for row in rows:
            subcategory_name = PRODUCT_SUBCATEGORIES_MAP.get(product_subcategory_key, "Categoria Desconocida")
            
            formatted_addition_timestamp = None
            row_dict = row._asdict() 

            if row_dict.get('addition_timestamp'):
                try:
                    formatted_addition_timestamp = row_dict['addition_timestamp'].isoformat()
                except AttributeError:
                    formatted_addition_timestamp = str(row_dict['addition_timestamp']) # Fallback

            results.append({
                "product_alternate_key": row_dict['product_alternate_key'],
                "english_product_name": row_dict['english_product_name'],
                "category_key": product_subcategory_key,
                "category_name": subcategory_name, # Anade el nombre legible
                "color": row_dict['color'],
                "addition_timestamp": formatted_addition_timestamp
            })
        
        if not results:
            return jsonify({"message": f"No se encontraron productos recientes para la categoria {product_subcategory_key}"}), 404
        
        return jsonify({
            "product_subcategory_key": product_subcategory_key,
            "category_name": PRODUCT_SUBCATEGORIES_MAP.get(product_subcategory_key, "Categoria Desconocida"),
            "recent_products": results
        }), 200
    except Exception as e:
        print(f"Error al consultar latest_product_category_trends: {e}")
        return jsonify({"error": f"Error al consultar productos por categoria: {str(e)}"}), 500

# --- Ejecucion de la API ---
if __name__ == '__main__':
    get_cassandra_session()
    app.run(
        debug=os.getenv('API_DEBUG', 'True').lower() == 'true',
        host=os.getenv('API_HOST', '0.0.0.0'),
        port=int(os.getenv('API_PORT', '5000'))
    )
