import os
import glob
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv

# Cargar las variables de entorno desde el archivo .env que está un nivel arriba
load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

def findLatestCSV(nomenclatura=None, directorio=None, encoding=None):
    """
    Encuentra y carga el archivo CSV más reciente que coincide con la nomenclatura dada en el directorio especificado.

    Parámetros:
        nomenclatura (str): Nomenclatura base de los archivos CSV.
        directorio (str): Directorio donde buscar los archivos CSV.
        encoding (str): Encoding para leer el archivo CSV. Por defecto es 'utf-8'.

    Si no se pasan los parámetros, se toman desde las variables de entorno:
    - NOMENCLATURA
    - DIRECTORIO
    - ENCODING

    Retorna:
        DataFrame o None: Retorna un DataFrame con los datos del CSV más reciente o None si no se encuentra ningún archivo.

    Ejecuta:
        df = findLatestCSV("dataCourses", "./", "latin-1")
    """
    # Tomar valores del .env si no se especifican
    nomenclatura = os.getenv("NOMENCLATURA", nomenclatura if nomenclatura else "dataCourses")
    directorio = os.getenv("DIRECTORIO", directorio if directorio else "./../data")
    encoding = os.getenv("ENCODING", encoding if encoding else "latin-1")
    # Construir el patrón de búsqueda
    patron_archivos = os.path.join(directorio, f"{nomenclatura}_*.csv")
    lista_archivos = glob.glob(patron_archivos)
    if not lista_archivos:
        print("No se encontró ningún archivo CSV.")
        return None
    archivos_recientes = sorted(lista_archivos, key=os.path.getctime, reverse=True)
    archivo_mas_reciente = archivos_recientes[0]
    df = pd.read_csv(archivo_mas_reciente, encoding=encoding)
    return df

def processDataCourses(nomenclatura=None, directorio=None, filterMembership=None, directorio_salida=None, encoding=None):
    """
    Procesa los datos de cursos a partir del archivo CSV más reciente.

    Parámetros:
        nomenclatura (str): Nomenclatura base de los archivos CSV.
        directorio (str): Directorio donde buscar los archivos CSV.
        filterMembership (str): Nombre de la membresía para filtrar los datos.
        directorio_salida (str): Directorio donde se guardará el archivo CSV resultante.
        encoding (str): Encoding para leer el archivo CSV. Por defecto es 'utf-8'.
    
    Si no se pasan los parámetros, se toman desde las variables de entorno:
    - NOMENCLATURA
    - DIRECTORIO
    - FILTER_MEMBERSHIP
    - DIRECTORIO_SALIADA
    - ENCODING

    Retorna:
        CSV procesado con los datos filtrados y seleccionados.

    Ejecuta:
        processDataCourses("Courses", "./", "Premiun", "./salida", "latin-1")
    """
    # Tomar valores del .env si no se especifican
    nomenclatura = os.getenv("NOMENCLATURA", nomenclatura if nomenclatura else "dataCourses")
    directorio = os.getenv("DIRECTORIO", directorio if directorio else "./../data")
    filterMembership = os.getenv("FILTER_MEMBERSHIP", filterMembership if filterMembership else "Dalia Pro Mensual")
    directorio_salida = os.getenv("DIRECTORIO_SALIADA", directorio_salida if directorio_salida else "./../data")
    encoding = os.getenv("ENCODING", encoding if encoding else "latin-1")
    # Ejecuta findLatestCSV con las variables obtenidas
    df = findLatestCSV(nomenclatura, directorio, encoding)
    if df is None:
        print("No data to process.")
        return
    # Filtrar por el nombre de la membresía
    df_filtered = df[df['Membership Names'] == filterMembership]
    # Seleccionar las columnas deseadas
    columnas_deseadas = [
        'Email', 'First Name', 'Last Name', 'Last Login Date',
        'Course', 'Courses Count', 'lessonName', 'isCompleted'
    ]
    df_selected = df_filtered[columnas_deseadas]
    # Renombrar columnas
    columnas_nuevas = {
        'Email': 'email',
        'First Name': 'Nombre(s)',
        'Last Name': 'Apellido(s)',
        'Last Login Date': 'Fecha ultimo ingreso',
        'Course': 'Curso',
        'Courses Count': 'Cursos asignados',
        'lessonName': 'Leccion',
        'isCompleted': 'Completo'
    }
    df_renamed = df_selected.rename(columns=columnas_nuevas)
    # Agrupación y agregación
    aggregation_functions = {
        'Fecha ultimo ingreso': 'last',
        'Curso': lambda x: ', '.join(x.unique()),
        'Cursos asignados': 'first',
        'Leccion': lambda x: ', '.join(x.astype(str).unique()),
    }
    df_grouped = df_renamed.groupby(['email','Nombre(s)', 'Apellido(s)']).agg(aggregation_functions).reset_index()
    # Separa cursos y lecciones
    df_grouped['Cursos'] = df_grouped['Curso'].apply(lambda x: ', '.join(set(x.split(', '))))
    df_grouped['Lecciones'] = df_grouped['Leccion'].apply(lambda x: ', '.join(set(x.split(', '))))
    # Elimina las columnas originales de cursos y lecciones
    df_grouped = df_grouped.drop(['Curso', 'Leccion'], axis=1)
    # Combina Nombre(s) y Apellido(s)
    df_grouped['Nombre completo'] = df_grouped['Nombre(s)'] + ' ' + df_grouped['Apellido(s)']
    df_grouped = df_grouped.drop(columns=['Nombre(s)', 'Apellido(s)'])
    # Reorganiza las columnas
    cols = df_grouped.columns.tolist()
    cols.insert(0, cols.pop(cols.index('Nombre completo')))
    df_grouped = df_grouped[cols]
    # Guardar el archivo con timestamp
    timestamp = datetime.now().strftime("%d_%m_%y_%H_%M")
    filename = f"groupby_name_{timestamp}.csv"
    output_path = os.path.join(directorio_salida, filename)
    df_grouped.to_csv(output_path, encoding=encoding, index=False)
    print(f"Archivo guardado como {output_path}")
# Llamada a la función
processDataCourses()