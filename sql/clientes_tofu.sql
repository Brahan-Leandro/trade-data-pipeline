SELECT DISTINCT c.nombreEmpresa
FROM pedidos_detalles_df pd
JOIN productos_df p ON pd.idProducto = p.idProducto --nombre producto
JOIN pedidos_df pe ON pd.idPedido = pe.idPedido --id_Cliente
JOIN clientes_df c ON pe.idCliente = c.idCliente --nombre del cliente
WHERE p.idProducto = 14 -- El idProducto del Tofu es 14