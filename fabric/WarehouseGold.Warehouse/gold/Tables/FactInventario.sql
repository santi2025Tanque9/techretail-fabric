CREATE TABLE [gold].[FactInventario] (
    [tienda_key]          INT           NULL,
    [fecha_key]           INT           NULL,
    [categoria]           VARCHAR (MAX) NULL,
    [cantidad_disponible] INT           NULL,
    [sin_stock]           BIT           NULL
);


GO