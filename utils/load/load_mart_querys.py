from google.cloud import bigquery
from dotenv import load_dotenv
import os

def load_data_to_bigquery(dataframe,  dataset, tabla):
    """
    Abre la conexión a BigQuery sube los datamarts y finaliza la conexión
    """   
    try:
        client = bigquery.Client()
        table_ref = client.dataset(dataset).table(tabla)
        table = bigquery.Table(table_ref)
        table = client.create_table(table, exists_ok=True)
        full_table_name = f"{client.project}.{dataset}.{tabla}"
        dataframe.to_gbq(destination_table=full_table_name, project_id=client.project, if_exists='replace')
    except ValueError as e:
        print(f"Error al subir los datamarts a BigQuery: {e}")
    finally:
        client.close()

def load_data_marts(df_pedidos_ultimos_6_meses, df_total_ventas_por_categoria, df_clientes_tofu, df_top1_transportistas_beverages, df_top2_transportistas_beverages):
    """
    Toma las tablas curadas del GSheets y se suben a la zona de curado en BigQuery
    """
    tables = [
        ("cnt_pedidos_ultimos_6_meses", df_pedidos_ultimos_6_meses),
        ("cnt_ventas_por_categorias", df_total_ventas_por_categoria),
        ("clientes_tofu", df_clientes_tofu),
        ("top_transportistas_v1", df_top1_transportistas_beverages),
        ("top_transportistas_v2", df_top2_transportistas_beverages)
    ]
    dataset = "mart_ops_envios"
    for table_name, dataframe in tables:
        load_data_to_bigquery(dataframe, dataset, table_name)
    print("Proceso de carga de datamarts existoso")
    

