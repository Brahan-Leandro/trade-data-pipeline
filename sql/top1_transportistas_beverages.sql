WITH transportistas AS(
    SELECT DISTINCT dpd.idPedido, dt.nombreEmpresa, dc.nombreCategoria, pd.fechaEnvio
    FROM pedidos_detalles_df dpd
    LEFT JOIN pedidos_df pd ON dpd.idPedido = pd.idPedido
    LEFT JOIN transportistas_df dt ON pd.idTransportista = dt.idTransportista
    LEFT JOIN productos_df dp ON dpd.idProducto = dp.idProducto
    LEFT JOIN categorias_df dc ON dp.idCategoria = dc.idCategoria
    WHERE dc.idCategoria = 1 -- El idCategoria de Beverage es 1
        AND pd.fechaEnvio IS NOT NULL
)
SELECT nombreEmpresa, COUNT(*) AS total_pedidos_beverage
FROM transportistas
GROUP BY nombreEmpresa
ORDER BY 2 DESC