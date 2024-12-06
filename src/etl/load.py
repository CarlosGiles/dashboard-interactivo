# src/etl/load.py
import pandas as pd
import os
from pathlib import Path
from dotenv import load_dotenv
from datetime import datetime

"""Cargamos las variables de entorno"""
# Ruta al archivo .env en la raíz del proyecto
BASE_DIR = Path(__file__).resolve().parent.parent.parent
env_path = BASE_DIR / '.env'
load_dotenv(dotenv_path=env_path)

# Damos formato al nombre del archivo CSV de salida
now = datetime.now()
formatted_datetime = now.strftime('dataCourses_%d_%m_%y_%H_%M.csv')

# Directorio de datos
DATA_DIR = BASE_DIR / 'data'
OUTPUT_CSV = os.getenv('OUTPUT_CSV')
OUTPUT_CSV_PATH = DATA_DIR / formatted_datetime

"""Importamos funciones de otros módulos"""
from .extract import (
    fetchDataFromAPI,
    loadDatasets,
    fetchLessonsFromAPI,
    USERS_API_URL,
    LESSONS_API_URL,
    COURSE_DATASET_PATH,
    MEMBERSHIP_DATASET_PATH,
    createDataFrameFromAPI
)
from .transform import (
    extractUserDetails,
    transformToDataFrame,
    handleNaNEmptyValues,
    mergeDataframes,
    fillMissingValues,
    convertAndFormatDatetime,
    cleanupDataFrame,
    csvFromDF
)

"""Definimos el main
Esta es la función principal que coordina las llamadas a las demás funciones, obtiene datos, los procesa y devuelve el DataFrame final.
"""
def main():
    jsonData = fetchDataFromAPI(USERS_API_URL)
    coursesDetails, membershipDetails = loadDatasets(COURSE_DATASET_PATH, MEMBERSHIP_DATASET_PATH)

    allRows = []
    for item in jsonData:
        userRows = extractUserDetails(item, membershipDetails, coursesDetails)
        allRows.extend(userRows)
    df = transformToDataFrame(allRows)
    df = handleNaNEmptyValues(df)
    return df

"""Ejecución
Ejecuta el proceso de obtención, unión, llenado y limpieza de un DataFrame base.
"""
def trackingModule(base_df):
    lessons_df = fetchLessonsFromAPI(LESSONS_API_URL)
    merged_df = mergeDataframes(base_df, lessons_df)
    cols_from_lessons = ['_id', 'lessonID', 'userID', 'courseID', 'createdAt', 'updatedAt', 'lessonName']
    merged_df = fillMissingValues(merged_df, cols_from_lessons)
    merged_df = convertAndFormatDatetime(merged_df, 'createdAt', 'Fecha primer ingreso', 'Hora primer ingreso')
    merged_df = convertAndFormatDatetime(merged_df, 'updatedAt', 'Fecha ultimo ingreso', 'Hora ultimo ingreso')
    columns_to_drop = ['updatedAt', 'userID', 'courseID', 'createdAt', '_id', 'Membership IDs']
    merged_df = cleanupDataFrame(merged_df, columns_to_drop)
    return merged_df

"""Ejecución del hilo principal"""
if __name__ == "__main__":
    df_base = main()
    final_df = trackingModule(df_base)
    csvFromDF()
    """Comprobación del módulo"""
    # print(final_df.tail(5))
    # Guardamos el DataFrame final en un archivo CSV en el directorio data/
    final_df.to_csv(OUTPUT_CSV_PATH, index=False, encoding='utf_8_sig')

"""
Ejecutar este script desde /src con el siguiente comando:
python -m etl.load
"""