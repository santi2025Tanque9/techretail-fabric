CREATE TABLE [gold].[real_vs_presupuesto] (
    [tienda_id]        INT             NULL,
    [anio_mes]         VARCHAR (MAX)   NULL,
    [venta_total_real] DECIMAL (14, 2) NULL,
    [meta_venta]       DECIMAL (12, 2) NULL,
    [variacion_pct]    DECIMAL (10, 2) NULL
);


GO