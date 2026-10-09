CREATE TABLE [gold].[DimCliente] (
    [cliente_key]     INT           NULL,
    [cliente_id]      INT           NULL,
    [nombre_completo] VARCHAR (MAX) NULL,
    [fecha_registro]  DATE          NULL,
    [tier_lealtad]    VARCHAR (MAX) NULL,
    [ciudad]          VARCHAR (MAX) NULL
);


GO