CREATE TABLE [gold].[DimFecha] (
    [fecha_key]        INT           NULL,
    [fecha]            DATE          NOT NULL,
    [anio]             INT           NOT NULL,
    [mes]              INT           NOT NULL,
    [trimestre]        INT           NOT NULL,
    [nombre_mes]       VARCHAR (MAX) NULL,
    [anio_mes]         VARCHAR (MAX) NOT NULL,
    [dia_semana]       VARCHAR (MAX) NULL,
    [dia_semana_num]   INT           NULL,
    [es_fin_de_semana] BIT           NULL
);


GO