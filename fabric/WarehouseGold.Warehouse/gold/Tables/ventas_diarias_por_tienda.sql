CREATE TABLE [gold].[ventas_diarias_por_tienda] (
    [tienda_key]             INT             NULL,
    [fecha_key]              INT             NULL,
    [venta_total]            DECIMAL (14, 2) NULL,
    [unidades]               INT             NULL,
    [cantidad_transacciones] INT             NOT NULL
);


GO