from google.cloud import bigquery
from dotenv import load_dotenv
import os

def load_data_to_bigquery(dataframe,  dataset, tabla): 
    """
    Abre la conexión a BigQuery sube los datos curados y finaliza la conexión
    """ 
    try:
        client = bigquery.Client()
        table_ref = client.dataset(dataset).table(tabla)
        table = bigquery.Table(table_ref)
        table = client.create_table(table, exists_ok=True)
        full_table_name = f"{client.project}.{dataset}.{tabla}"
        dataframe.to_gbq(destination_table=full_table_name, project_id=client.project, if_exists='replace')
    except ValueError as e:
        print(f"Error al subir la data curada a BigQuery: {e}")
    finally:
        client.close()

def load_data_from_google_sheets(categorias_df, pedidos_df, empleados_df, clientes_df, pedidos_detalles_df, productos_df, transportistas_df):
    """
    Toma las tablas curadas del GSheets y se suben a la zona de curado en BigQuery
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
    dataset = "curated_ops_envios" 
    for table_name, dataframe in tables:
        load_data_to_bigquery(dataframe, dataset, table_name)
    print("Proceso de carga de datos curados existoso")