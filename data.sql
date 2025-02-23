CREATE TABLE CdcChange (
    Id INT PRIMARY KEY,          -- Primary Key (Assumption: ID is unique)
    Name NVARCHAR(255) NOT NULL, -- Stores name (Assumption: max length 255)
    CreatedAt DATETIME NOT NULL, -- Stores record creation timestamp
    SeqVal BIGINT NOT NULL,      -- Stores CDC sequence value
    OperationCode INT NOT NULL   -- Stores raw CDC operation code
);


EXEC sys.sp_cdc_enable_table  
    @source_schema = 'dbo',  
    @source_name = 'CdcChange',  
    @role_name = NULL,  
    @capture_instance = 'CdcChange_CDC';


	SELECT * FROM cdc.change_tables;

	INSERT INTO CdcChange (Id, Name, CreatedAt, SeqVal, OperationCode) 
VALUES 
    (5, 'John Doe', '2024-02-23 10:15:00', 10001, 1)
