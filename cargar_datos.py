import pandas as pd
from sqlalchemy import create_engine

db_user = "admin_negocios"
db_password = "Maybeyes13!"
db_host = "34.59.177.251"
db_port = "3306"
db_name = "inteligencia_negocios"

csv_file_path = "ai_job_dataset.csv"


table_name = "trabajos_ia"

try:
    connection_string = f"mysql+pymysql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    engine = create_engine(connection_string)

    print(f"Leyendo el archivo CSV: {csv_file_path}...")
    df = pd.read_csv(csv_file_path)
    print(f"Se encontraron {len(df)} registros en el archivo.")
    print(f"Cargando datos en la tabla '{table_name}'...")
    df.to_sql(table_name, con=engine, if_exists='append', index=False)

    print("✅ ¡Carga de datos completada exitosamente!")

except FileNotFoundError:
    print(f"Error: No se encontró el archivo en la ruta '{csv_file_path}'.")
    print("Asegúrate de que el nombre y la ubicación del archivo son correctos.")
except Exception as e:
    print(f"Ocurrió un error: {e}")