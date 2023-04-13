import boto3
import pandas as pd
import pymssql

# Function to setup the connectionwith the S3 resource.
def configure_s3_client():
    # Configure S3 credentials
    aws_access_key_id = 'AKIAQPFFDDWRVVZHEF6S'
    aws_secret_access_key = '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'
    aws_region_name = 'us-east-2'

    # Create an S3 client
    s3_client = boto3.client('s3',
                            aws_access_key_id=aws_access_key_id,
                            aws_secret_access_key=aws_secret_access_key,
                            region_name=aws_region_name)
    
    return s3_client


# Function to setup the connection with the transactional SQL Server DB and create a TestTable .
def Connect_AirPollutiondb():
    # Connect to the SQL Server RDS instance
    connection={
        'host': 'cdp-of-transac-db-sqlserv.cxvu5cgewkxj.us-east-2.rds.amazonaws.com',
        'username': 'superadmin',
        'password': 'cdp-Admin3',
        'db': 'AirPollution'
    }

    #Creating a connection
    connection = pymssql.connect(connection['host'],connection['username'],connection['password'],connection['db'])

    # Verificar si la tabla 'TestTable' ya existe
    cursor = connection.cursor()
    cursor.execute("SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'TestTable3';")
    table_exists = cursor.fetchone()[0]

    if table_exists:
        print("La tabla 'TestTable' ya existe.")
    else:
        # Crear una nueva tabla llamada "TestTable3"
        cursor.execute("""
            CREATE TABLE TestTable3 (
                id INT PRIMARY KEY,
                nombre VARCHAR(255),
                valor INT
            );
        """)
        print("Tabla 'TestTable3' creada exitosamente.")


    connection.commit()
    connection.close()
    return