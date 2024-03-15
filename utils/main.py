from google.cloud import bigquery
from dotenv import load_dotenv
import os
from extract.extract_gsheet import extract_data_from_google_sheets
from transform.transform_gsheet import clean_data_from_google_sheets
from transform.transform_bigquery import execute_sql_query_from_file
from load.load_mart_querys import load_data_marts
from load.load_curated_gsheet import load_data_from_google_sheets


dataframes = extract_data_from_google_sheets()

categorias_df, pedidos_df, empleados_df, clientes_df, pedidos_detalles_df, productos_df, transportistas_df = clean_data_from_google_sheets(dataframes)

load_data_from_google_sheets(categorias_df, pedidos_df, empleados_df, clientes_df, pedidos_detalles_df, productos_df, transportistas_df)

df_pedidos_ultimos_6_meses, df_total_ventas_por_categoria, df_clientes_tofu, df_top1_transportistas_beverages,\
      df_top2_transportistas_beverages = execute_sql_query_from_file(categorias_df, pedidos_df, empleados_df,\
                                        clientes_df, pedidos_detalles_df, productos_df, transportistas_df )

load_data_marts(df_pedidos_ultimos_6_meses, df_total_ventas_por_categoria, df_clientes_tofu, df_top1_transportistas_beverages, df_top2_transportistas_beverages)