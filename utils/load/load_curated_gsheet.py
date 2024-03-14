from google.cloud import bigquery
from dotenv import load_dotenv
import os

def load_data_to_bigquery(dataframe,  dataset, tabla):  
    try:
        #Configura el cliente de BigQuery
        client = bigquery.Client()
        #Especifica la tabla destino en BigQuery
        table_ref = client.dataset(dataset).table(tabla)
        #Crea la tabla si no existe
        table = bigquery.Table(table_ref)
        table = client.create_table(table, exists_ok=True)
        #Forma el nombre completo de la tabla en el formato correcto
        full_table_name = f"{client.project}.{dataset}.{tabla}"
        #Carga el DataFrame en BigQuery
        dataframe.to_gbq(destination_table=full_table_name, project_id=client.project, if_exists='replace')
    except ValueError as e:
        print(f"Error al formatear el DataFrame de transportistas: {e}")
    #Cierra la conexión a BigQuery
    client.close()

def load_data_from_google_sheets(categorias_df, pedidos_df, empleados_df, clientes_df, pedidos_detalles_df, productos_df, transportistas_df):
    """
    Extrae los datos de las hojas de cálculo de Google y los devuelve como un diccionario de DataFrames.
    """
    tables = [
        ("dim_categorias", categorias_df),
        ("fact_pedidos", pedidos_df),
        ("dim_empleados", empleados_df),
        ("dim_clientes", clientes_df),
        ("dim_pedidos_detalles", pedidos_detalles_df),
        ("dim_productos", productos_df),
        ("dim_transportistas", transportistas_df)
    ]
    dataset = "curated_ops_envios" #Nombre del esquema en BigQuery
    for table_name, dataframe in tables:
        load_data_to_bigquery(dataframe, dataset, table_name)
    


"""
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
"""

