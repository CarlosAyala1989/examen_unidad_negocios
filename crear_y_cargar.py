import os
import pandas as pd
import mysql.connector
from mysql.connector import Error

def map_dtype_to_sql(dtype):
    """Traduce los tipos de datos de Pandas a tipos de datos de MySQL."""
    if "int" in str(dtype):
        return "BIGINT"
    elif "float" in str(dtype):
        return "DOUBLE"
    elif "datetime" in str(dtype):
        return "DATETIME"
    elif "object" in str(dtype):
        return "VARCHAR(255)" # Un valor por defecto seguro para texto
    else:
        return "VARCHAR(255)"

def crear_y_cargar():
    try:
        # --- 1. Lee la configuración desde las variables de entorno ---
        db_host = os.getenv('DB_HOST')
        db_port = os.getenv('DB_PORT')
        db_database = os.getenv('DB_DATABASE')
        db_user = os.getenv('DB_USER')
        db_password = os.getenv('DB_PASSWORD')
        csv_file_path = os.getenv('CSV_FILENAME', 'ai_job_dataset.csv')
        table_name = os.getenv('TABLE_NAME', 'jobs')

        print(f"Iniciando el proceso para el archivo '{csv_file_path}' en la tabla '{table_name}'.")

        # --- 2. Lee y analiza el archivo CSV con Pandas ---
        print(f"Leyendo el archivo CSV: {csv_file_path}...")
        df = pd.read_csv(csv_file_path)
        
        # Convierte valores vacíos a None para que SQL los interprete como NULL
        df = df.where(pd.notnull(df), None)

        # --- 3. Genera la sentencia SQL 'CREATE TABLE' dinámicamente ---
        column_definitions = []
        for col in df.columns:
            # Limpia los nombres de las columnas para que sean compatibles con SQL
            clean_col_name = "".join(c for c in col if c.isalnum() or c == '_').strip()
            sql_type = map_dtype_to_sql(df[col].dtype)
            column_definitions.append(f"`{clean_col_name}` {sql_type}")
        
        create_table_query = f"CREATE TABLE IF NOT EXISTS `{table_name}` ({', '.join(column_definitions)});"
        
        print("\nSentencia CREATE TABLE generada:")
        print(create_table_query)

        # --- 4. Conecta a la base de datos, crea la tabla e inserta los datos ---
        print("\nConectando a la base de datos MySQL...")
        conn = mysql.connector.connect(
            host=db_host,
            port=int(db_port),
            database=db_database,
            user=db_user,
            password=db_password
        )

        if conn.is_connected():
            print("¡Conexión exitosa!")
            cursor = conn.cursor()
            
            # Ejecuta el comando para crear la tabla
            print(f"Creando la tabla '{table_name}' si no existe...")
            cursor.execute(create_table_query)
            print("Tabla verificada/creada con éxito.")

            # Prepara la inserción de datos
            cols = ", ".join([f"`{col}`" for col in df.columns])
            placeholders = ", ".join(["%s"] * len(df.columns))
            insert_query = f"INSERT INTO `{table_name}` ({cols}) VALUES ({placeholders})"
            
            print(f"Insertando {len(df)} filas...")
            for index, row in df.iterrows():
                cursor.execute(insert_query, tuple(row))
            
            conn.commit()
            print(f"¡Éxito! Se han cargado {cursor.rowcount} filas en la tabla '{table_name}'.")

    except Error as e:
        print(f"Error durante el proceso: {e}")
    
    finally:
        if 'conn' in locals() and conn.is_connected():
            cursor.close()
            conn.close()
            print("Conexión a MySQL cerrada.")

if __name__ == '__main__':
    crear_y_cargar()