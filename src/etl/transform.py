# src/etl/transform.py
import pandas as pd

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