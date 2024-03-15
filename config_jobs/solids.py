from dagster import op, job, Nothing, Out, Output
from utils.extract.extract_gsheet import extract_data_from_google_sheets
from utils.transform.transform_gsheet import clean_data_from_google_sheets
from utils.transform.transform_bigquery import execute_sql_query_from_file
from utils.load.load_mart_querys import load_data_marts
from utils.load.load_curated_gsheet import load_data_from_google_sheets

@op(description="Correrá la extracción del G-Sheets")
def load_info_data(context):
    try:
        context.log.info("Inicio de extracción")
        dataframes = extract_data_from_google_sheets()
        context.log.info("Extracción con exito")
        return dataframes
    except Exception as e:
        context.log.info(f"Extracción fallida, error {e}")


@op(description="Correrá la limpieza de la data extraida y con esto tendremos la data curada",
out={
    "categorias_df": Out(),
    "pedidos_df": Out(),
    "empleados_df": Out(),
    "clientes_df": Out(),
    "pedidos_detalles_df": Out(),
    "productos_df": Out(),
    "transportistas_df": Out()
})
def transform_info_data(context, dataframes):
    try:
        context.log.info("Inicio de la limpieza de datos")
        # Suponiendo que clean_data_from_google_sheets devuelve los 7 DataFrames en el orden correcto
        dfs = clean_data_from_google_sheets(dataframes)
        for i, name in enumerate(["categorias_df", "pedidos_df", "empleados_df", "clientes_df", "pedidos_detalles_df", "productos_df", "transportistas_df"]):
            yield Output(dfs[i], name)
        context.log.info("Limpieza de datos exitosa")
    except Exception as e:
        context.log.error(f"Limpieza de datos fallida, error {e}")


@op(description="Subira los datos curados a BQ en el esquema curated_ops_envios")
def load_data_curada_bq(context, categorias_df, pedidos_df, empleados_df, clientes_df, pedidos_detalles_df, productos_df, transportistas_df) -> Nothing:
    try:
        context.log.info("Cargando data curada a BQ en el esquema curated_ops_envios")
        load_data_from_google_sheets(categorias_df, pedidos_df, empleados_df, clientes_df, pedidos_detalles_df, productos_df, transportistas_df)
        context.log.info("Carga de data curada a BQ exitosa")
    except Exception as e:
        context.log.info(f"Carga de datos curada a BQ fallida, error {e}")


@op(description="Transformara la data curada en las tablas marts",
out={
    "df_pedidos_ultimos_6_meses": Out(),
    "df_total_ventas_por_categoria": Out(),
    "df_clientes_tofu": Out(),
    "df_top1_transportistas_beverages": Out(),
    "df_top2_transportistas_beverages": Out()
})
def transform_data_marts(context, categorias_df, pedidos_df, empleados_df, clientes_df, pedidos_detalles_df, productos_df, transportistas_df) -> Nothing:
    try:
        context.log.info("Comenzando la transformación de la data curada, para transformarla en las tablas marts")
        dfs= execute_sql_query_from_file(categorias_df, pedidos_df, empleados_df,\
                                        clientes_df, pedidos_detalles_df, productos_df, transportistas_df )
        for i, name in enumerate(["df_pedidos_ultimos_6_meses", "df_total_ventas_por_categoria", "df_clientes_tofu",\
                                   "df_top1_transportistas_beverages","df_top2_transportistas_beverages"]):
            yield Output(dfs[i], name)
        context.log.info("Transformación de la data curada exitosa")
    except Exception as e:
        context.log.info(f"Transformación de la data curada fallida, error {e}")


@op(description="Cargar las tablas marts a BQ en el esquema mart_ops_envios")
def load_tables_marts_bq(context, df_pedidos_ultimos_6_meses, df_total_ventas_por_categoria, df_clientes_tofu, df_top1_transportistas_beverages, df_top2_transportistas_beverages) -> Nothing:
    try:
        context.log.info("Comenzando la cargas de las tablas marts a BQ en el esquema mart_ops_envios")
        load_data_marts(df_pedidos_ultimos_6_meses, df_total_ventas_por_categoria, df_clientes_tofu, df_top1_transportistas_beverages, df_top2_transportistas_beverages)
        context.log.info("Cargas de las tablas marts a BQ exitosa")
    except Exception as e:
        context.log.info(f"Cargas de las tablas marts a BQ fallida, error {e}")



@job(tags={"Owner": "Brahan Soledad", "Process": "Extract - Transform - Load", "Country": "Colombia"},
     description="Prueba")
def pipeline():
    dataframes = load_info_data()
    results = transform_info_data(dataframes)
    marts = transform_data_marts(
        categorias_df=results.categorias_df,
        pedidos_df=results.pedidos_df,
        empleados_df=results.empleados_df,
        clientes_df=results.clientes_df,
        pedidos_detalles_df=results.pedidos_detalles_df,
        productos_df=results.productos_df,
        transportistas_df=results.transportistas_df
    )
    load_data_curada_bq(
        categorias_df=results.categorias_df,
        pedidos_df=results.pedidos_df,
        empleados_df=results.empleados_df,
        clientes_df=results.clientes_df,
        pedidos_detalles_df=results.pedidos_detalles_df,
        productos_df=results.productos_df,
        transportistas_df=results.transportistas_df
    )
    load_tables_marts_bq(
        df_pedidos_ultimos_6_meses=marts.df_pedidos_ultimos_6_meses,
        df_total_ventas_por_categoria=marts.df_total_ventas_por_categoria,
        df_clientes_tofu=marts.df_clientes_tofu,
        df_top1_transportistas_beverages=marts.df_top1_transportistas_beverages,
        df_top2_transportistas_beverages=marts.df_top2_transportistas_beverages
    )