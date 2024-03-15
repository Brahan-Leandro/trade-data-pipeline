from pandasql import sqldf
import pandas as pd

def read_sql_query(path_to_sql_file):
    """
    Función para cargar una consulta SQL desde un archivo.
    """
    with open(path_to_sql_file, 'r') as file:
        query = file.read()
    return query


def execute_sql_query_from_file(categorias_df, pedidos_df, empleados_df, clientes_df, pedidos_detalles_df, productos_df, transportistas_df ):
    """
    Ejecuta las consultas SQL almacenadas en los archivos dentro de la carpeta "sql" sobre los dataframes proporcionados.
    """
    try: 
        query1 = read_sql_query('./sql/pedidos_ultimos_6_meses.sql')
        df_pedidos_ultimos_6_meses = sqldf(query1, locals())

        query2 = read_sql_query('./sql/total_ventas_por_categoria.sql')
        df_total_ventas_por_categoria = sqldf(query2)

        query3 = read_sql_query('./sql/clientes_tofu.sql')
        df_clientes_tofu = sqldf(query3)

        query4 = read_sql_query('./sql/top1_transportistas_beverages.sql')
        df_top1_transportistas_beverages = sqldf(query4)

        query5 = read_sql_query('./sql/top2_transportistas_beverages.sql')
        df_top2_transportistas_beverages = sqldf(query5)
            
        print("Proceso de creación de marts exitoso")
        return df_pedidos_ultimos_6_meses, df_total_ventas_por_categoria, df_clientes_tofu, df_top1_transportistas_beverages, df_top2_transportistas_beverages
    except Exception as e:
        print(f"Error al ejecutar los querys: {str(e)}")
        return None, None, None, None, None

