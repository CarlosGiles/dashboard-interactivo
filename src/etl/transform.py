# src/etl/transform.py
import pandas as pd
from dotenv import load_dotenv
import os
from datetime import datetime
from .extract import createDataFrameFromAPI

"""Extract user details
Dado un objeto item (usuario) y detalles sobre las membresías y cursos.
Esta función extrae todos los detalles necesarios del usuario y devuelve una lista de filas que se agregarán al DataFrame final.
"""
def extractUserDetails(item, membershipDetails, coursesDetails):
    userCourses = item["courses"]
    userSubscriptions = item["subscriptions"]
    membershipIds = [subscription.get("membershipId", 'N/A') for subscription in userSubscriptions]
    membershipNames = [
        membershipDetails[membershipDetails['_id'] == membershipId]['name'].values[0]
        if membershipId in membershipDetails['_id'].values else 'N/A'
        for membershipId in membershipIds
    ]
    lastLogin = item.get("lastLogin", "N/A")
    userId = item["_id"]
    email = item["email"]
    firstName = item["firstName"]
    lastName = item["lastName"]

    if not userCourses:
        return [(userId, email, firstName, lastName, ', '.join(membershipNames), lastLogin, 'Sin curso', '-', '-', 0, '-', '-', '-', '-', ', '.join(membershipIds))]

    rows = []
    for course in userCourses:
        courseId = course["courseId"]
        if courseId in coursesDetails['_id'].values:
            courseDetailsRow = coursesDetails[coursesDetails['_id'] == courseId].iloc[0]
        else:
            courseDetailsRow = pd.Series(index=coursesDetails.columns)
        rows.append((
            userId,
            email,
            firstName,
            lastName,
            ', '.join(membershipNames),
            lastLogin,
            courseDetailsRow.get('_id','-'),
            courseDetailsRow.get('name', '-'),
            courseDetailsRow.get('type', '-'),
            len(userCourses),
            courseDetailsRow.get('status', '-'),
            courseDetailsRow.get('category', '-'),
            courseDetailsRow.get('level', '-'),
            courseDetailsRow.get('instructor', '-'),
            ', '.join(membershipIds)
        ))
    return rows

"""Transform data to dataFrame
Toma una lista de filas de datos y las transforma en un DataFrame.
También se encarga de las transformaciones específicas del DataFrame, como la conversión de fechas.
"""
def transformToDataFrame(dataRows):
    headers = ["ID", "Email", "First Name", "Last Name", "Membership Names", "Last Login", "ID Course", "Course", "Course Type", "Courses Count", "Status", "Category", "Level", "Instructor", "Membership IDs"]
    df = pd.DataFrame(dataRows, columns=headers)
    df['Last Login'] = pd.to_datetime(df['Last Login'], errors='coerce')
    df['Last Login'] = df['Last Login'].dt.tz_convert('America/Mexico_City')
    df['Last Login Date'] = df['Last Login'].dt.strftime('%d/%m/%Y')
    df['Last Login Hour'] = df['Last Login'].dt.strftime('%H:%M:%S')

    columnsOrder = [
        'ID', 'Email', 'First Name', 'Last Name', 'Membership Names',
        'Last Login Date', 'Last Login Hour', 'ID Course',
        'Course', 'Course Type', 'Courses Count', 'Status',
        'Category', 'Level', 'Instructor', 'Membership IDs'
    ]

    return df[columnsOrder]

"""Handle NaN or Empty values on a Dataframe"""
def handleNaNEmptyValues(df):
    df = df.fillna('-')
    return df

"""Merge DataFrames
Une dos DataFrames en función de ID y ID de curso.
"""
def mergeDataframes(base_df, lessons_df):
    merged_df = pd.merge(base_df, lessons_df, left_on=['ID', 'ID Course'], right_on=['userID', 'courseID'], how='left')
    return merged_df

"""Fill Missing Values
Llena los valores faltantes en un DataFrame.
"""
def fillMissingValues(df, cols_from_lessons):
    for col in cols_from_lessons:
        df[col].fillna("-", inplace=True)
    return df

"""Convert and Format Datetime
Convierte y formatea una columna de fecha y hora en un DataFrame.
"""
def convertAndFormatDatetime(df, column_name, new_date_name, new_time_name):
    df[column_name] = pd.to_datetime(df[column_name], errors='coerce')
    df[column_name] = df[column_name].dt.tz_convert('America/Mexico_City')
    df[new_date_name] = df[column_name].dt.strftime('%d/%m/%Y')
    df[new_time_name] = df[column_name].dt.strftime('%H:%M:%S')
    return df

"""Cleanup Dataframe
Limpia un DataFrame eliminando columnas específicas.
"""
def cleanupDataFrame(df, columns_to_drop):
    return df.drop(columns=columns_to_drop)

def csvFromDF(directorio_salida=None, encoding=None, nomenclatura_salida=None, url=None):
    """
    Crea un archivo CSV a partir de un DataFrame obtenido con la función createDataFrameFromAPI.
    Los parámetros directorio_salida, encoding y nomenclatura_salida son opcionales.
    Si no se proporcionan, se toman de las variables de entorno OUTPUT_DIRECTORY, OUTPUT_ENCODING y NOMENCLATURA_SALIDA.
    """
    # Cargar variables de entorno
    load_dotenv()
    # Si no se proporcionan los parámetros, se toman del .env
    if directorio_salida is None:
        directorio_salida = os.getenv("OUTPUT_DIRECTORY", "./../data")
    if encoding is None:
        encoding = os.getenv("OUTPUT_ENCODING", "latin-1")
    if nomenclatura_salida is None:
        nomenclatura_salida = os.getenv("NOMENCLATURA_SALIDA", "completados")
    # Obtener el DataFrame desde la API (con URL del parámetro o del .env)
    df = createDataFrameFromAPI(url=url)
    # Generar timestamp con el formato día, mes, año, hora y minuto
    timestamp = datetime.now().strftime("%d_%m_%y_%H_%M")
    # Crear el nombre del archivo de salida
    filename = f"{nomenclatura_salida}_{timestamp}.csv"
    # Crear el directorio de salida si no existe
    if not os.path.exists(directorio_salida):
        os.makedirs(directorio_salida, exist_ok=True)
    # Ruta completa del archivo de salida
    output_path = os.path.join(directorio_salida, filename)
    # Guardar el DataFrame en un CSV con el encoding especificado
    df.to_csv(output_path, encoding=encoding, index=False)
    print(f"Archivo guardado como {output_path}")