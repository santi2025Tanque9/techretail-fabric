CREATE TABLE [gold].[FactVentas] (
    [transaccion_id]    VARCHAR (MAX)   NULL,
    [fecha_key]         INT             NULL,
    [tienda_key]        INT             NULL,
    [producto_key]      INT             NULL,
    [empleado_key]      INT             NULL,
    [cliente_key]       INT             NOT NULL,
    [cantidad]          INT             NULL,
    [precio_unitario]   DECIMAL (10, 2) NULL,
    [descuento_pct]     INT             NULL,
    [venta_total]       DECIMAL (12, 2) NULL,
    [costo_total]       DECIMAL (12, 2) NULL,
    [requiere_revision] BIT             NOT NULL
);


GO