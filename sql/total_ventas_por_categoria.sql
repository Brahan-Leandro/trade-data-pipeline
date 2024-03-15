SELECT dc.nombreCategoria, COUNT(dpd.idProducto) AS TotalVentas
FROM pedidos_detalles_df as dpd
JOIN productos_df dp ON dpd.idProducto = dp.idProducto
JOIN categorias_df dc ON dp.idCategoria = dc.idCategoria
GROUP BY dc.idCategoria
ORDER BY 2 ASC