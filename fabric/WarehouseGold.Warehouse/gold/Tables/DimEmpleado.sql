CREATE TABLE [gold].[DimEmpleado] (
    [empleado_key]        INT             NOT NULL,
    [empleado_id]         INT             NULL,
    [nombre_completo]     VARCHAR (MAX)   NULL,
    [tienda_id]           INT             NULL,
    [puesto]              VARCHAR (MAX)   NULL,
    [fecha_ingreso]       DATE            NULL,
    [salario]             DECIMAL (10, 2) NULL,
    [documento_identidad] VARCHAR (MAX)   NULL
);


GO