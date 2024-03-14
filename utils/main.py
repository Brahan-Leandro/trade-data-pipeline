from google.cloud import bigquery
from dotenv import load_dotenv
import os

from extract.extract_gsheet import extract_data_from_google_sheets
from transform.transform_gsheet import clean_data_from_google_sheets
from load.load_curated_gsheet import load_data_from_google_sheets

dataframes = extract_data_from_google_sheets()
categorias_df, pedidos_df, empleados_df, clientes_df, pedidos_detalles_df, productos_df, transportistas_df = clean_data_from_google_sheets(dataframes)
load_data_from_google_sheets(categorias_df, pedidos_df, empleados_df, clientes_df, pedidos_detalles_df, productos_df, transportistas_df)

