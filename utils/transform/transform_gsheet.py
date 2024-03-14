import pandas as pd

def format_categorias(categorias_df):
    """
    Formatea el DataFrame de categorías.
    """
    try:
        categorias_df = categorias_df.astype({
            'idCategoria': 'int',
            'nombreCategoria': 'category',
            'descripcion': 'str'
        })
        return categorias_df
    except ValueError as e:
        print(f"Error al formatear el DataFrame de categorías: {e}")
    

def format_pedidos(pedidos_df):
    """
    Formatea el DataFrame de pedidos.
    """
    try:
        pedidos_df['flete'] = pedidos_df['flete'].str.replace(',', '.')
        for col in ['fechaPedido', 'fechaRequerida', 'fechaEnvio']:
            pedidos_df[col] = pedidos_df[col].str.replace('-', '/')
            pedidos_df[col] = pd.to_datetime(pedidos_df[col], errors='coerce')
        pedidos_df = pedidos_df.astype({
            'idPedido': 'int',
            'idCliente': 'str',
            'idEmpleado': 'int',
            'idTransportista': 'int',
            'flete': 'float'
        })
        return pedidos_df
    except ValueError as e:
        print(f"Error al formatear el DataFrame de pedidos: {e}")
    

def format_empleados(empleados_df):
    """
    Formatea el DataFrame de empleados.
    """
    try:
        empleados_df = empleados_df.astype({
            'idEmpleado': 'int',
            'nombreEmpleado': 'str',
            'titulo': 'category',
            'ciudad': 'category',
            'pais': 'category',
            'reportaA': 'Int64'
        })
        return empleados_df
    except ValueError as e:
        print(f"Error al formatear el DataFrame de empleados: {e}")
    

def format_clientes(clientes_df):
    """
    Formatea el DataFrame de clientes.
    """
    try:
        clientes_df = clientes_df.astype({
            'idCliente': 'str',
            'nombreEmpresa': 'str',
            'nombreContacto': 'str',
            'tituloContacto': 'category',
            'ciudad': 'category',
            'pais': 'category'
        })
        return clientes_df
    except ValueError as e:
        print(f"Error al formatear el DataFrame de clientes: {e}")
    

def format_pedidos_detalles(pedidos_detalles_df):
    """
    Formatea el DataFrame de detalles de pedidos.
    """
    try:
        pedidos_detalles_df['descuento'] = pedidos_detalles_df['descuento'].str.replace(',', '.')
        pedidos_detalles_df['idProducto'] = pedidos_detalles_df['idProducto'].str.replace(' ', '')
        pedidos_detalles_df['precioUnitario'] = pedidos_detalles_df['precioUnitario'].str.replace(',', '.')
        pedidos_detalles_df = pedidos_detalles_df.astype({
            'idPedido': 'int',
            'idProducto': 'int',
            'precioUnitario': 'float',
            'cantidad': 'int',
            'descuento': 'float'
        })
        return pedidos_detalles_df
    except ValueError as e:
        print(f"Error al formatear el DataFrame de pedidos detalles: {e}")
    

def format_productos(productos_df):
    """
    Formatea el DataFrame de productos.
    """
    try:
        productos_df['precioUnitario'] = productos_df['precioUnitario'].str.replace(',', '.')
        productos_df = productos_df.astype({
            'idProducto': int,
            'nombreProducto': str,
            'cantidadPorUnidad': str,
            'precioUnitario': float,
            'descontinuado': int,
            'idCategoria': int
        })
        return productos_df
    except ValueError as e:
        print(f"Error al formatear el DataFrame de productos: {e}")
    

def format_transportistas(transportistas_df):
    """
    Formatea el DataFrame de transportistas.
    """
    try:
        transportistas_df = transportistas_df.astype({
            'idTransportista': int,
            'nombreEmpresa': str
        })
        return transportistas_df
    except ValueError as e:
        print(f"Error al formatear el DataFrame de transportistas: {e}")
    

def clean_data_from_google_sheets(dataframes):
    """
    Limpia los datos de las hojas de cálculo de Google.
    """
    categorias_df = dataframes['categorias']
    pedidos_df = dataframes['pedidos']
    empleados_df = dataframes['empleados']
    clientes_df = dataframes['clientes']
    pedidos_detalles_df = dataframes['pedidos_detalles']
    productos_df = dataframes['productos']
    transportistas_df = dataframes['transportistas']

    categorias_df = format_categorias(categorias_df)
    pedidos_df = format_pedidos(pedidos_df)
    empleados_df = format_empleados(empleados_df)
    clientes_df = format_clientes(clientes_df)
    pedidos_detalles_df = format_pedidos_detalles(pedidos_detalles_df)
    productos_df = format_productos(productos_df)
    transportistas_df = format_transportistas(transportistas_df)
    print("Proceso de limpieza de datos de GSheets exitosa")

    return categorias_df, pedidos_df, empleados_df, clientes_df, pedidos_detalles_df, productos_df, transportistas_df
