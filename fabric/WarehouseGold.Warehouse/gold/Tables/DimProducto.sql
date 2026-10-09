CREATE TABLE [gold].[DimProducto] (
    [producto_key]    INT             NOT NULL,
    [sku]             VARCHAR (MAX)   NULL,
    [nombre_producto] VARCHAR (MAX)   NULL,
    [categoria]       VARCHAR (MAX)   NULL,
    [subcategoria]    VARCHAR (MAX)   NULL,
    [marca]           VARCHAR (MAX)   NULL,
    [costo_unitario]  DECIMAL (10, 2) NULL
);


GO