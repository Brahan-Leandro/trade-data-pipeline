WITH FechaUltimoPedido AS (
    SELECT MAX(fechaPedido) AS ultimaFechaEnvio FROM pedidos_df
),
RangoPedidosUltimos6Meses AS (
    SELECT *,
    strftime('%Y-%m', fechaPedido) AS MesEnvio
    FROM pedidos_df, FechaUltimoPedido
    WHERE strftime('%Y-%m', fechaPedido) >= DATE(ultimaFechaEnvio, '-6 months')
)
SELECT MesEnvio, COUNT(*) AS TotalPedidos
FROM RangoPedidosUltimos6Meses
GROUP BY MesEnvio
ORDER BY MesEnvio DESC