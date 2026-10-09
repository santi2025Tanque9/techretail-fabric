CREATE TABLE [gold].[DimTienda] (
    [tienda_key]            INT           NOT NULL,
    [tienda_id]             INT           NULL,
    [nombre_tienda]         VARCHAR (MAX) NULL,
    [nombre_tienda_display] VARCHAR (MAX) NULL,
    [ciudad]                VARCHAR (MAX) NULL,
    [pais]                  VARCHAR (MAX) NULL,
    [region]                VARCHAR (MAX) NULL,
    [formato_tienda]        VARCHAR (MAX) NULL,
    [fecha_apertura]        DATE          NULL
);


GO