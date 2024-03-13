from google.cloud import bigquery
from dotenv import load_dotenv
import os

# Cargar variables de entorno desde el archivo .env
load_dotenv()

# Obtener la ruta al archivo de credenciales desde las variables de entorno
credentials_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")

try:
    # Inicializar el cliente de BigQuery con las credenciales personalizadas
    client = bigquery.Client.from_service_account_json(credentials_path)
    print("Conexión exitosa a BigQuery.")
    
    # Resto del código para cargar datos a BigQuery...
except Exception as e:
    print("Error al conectarse a BigQuery:", e)

print("Ruta del archivo de credenciales:", credentials_path)
