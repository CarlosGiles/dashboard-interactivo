# src/etl/extract.py
import requests
import pandas as pd
from dotenv import load_dotenv
import os
from pathlib import Path

# Cargamos las variables de entorno con la ruta al archivo .env en la raíz del proyecto
BASE_DIR = Path(__file__).resolve().parent.parent.parent
env_path = BASE_DIR / '.env'
load_dotenv(dotenv_path=env_path)

# Declaramos e inicializamos las constantes desde variables de entorno
USERS_API_URL = os.getenv('USERS_API_URL')
COURSE_DATASET = os.getenv('COURSE_DATASET')
MEMBERSHIP_DATASET = os.getenv('MEMBERSHIP_DATASET')
LESSONS_API_URL = os.getenv('LESSONS_API_URL')

# Directorio de datos
DATA_DIR = BASE_DIR / 'data'

COURSE_DATASET_PATH = DATA_DIR / COURSE_DATASET
MEMBERSHIP_DATASET_PATH = DATA_DIR / MEMBERSHIP_DATASET

"""Fetch from API
Recibe una URL y devuelve el JSON obtenido de la API.
Encapsulando el proceso de hacer un request.
"""
def fetchDataFromAPI(url):
    response = requests.get(url)
    return response.json()

"""Load Datasets
Carga los datasets de cursos y membresías y devuelve dos DataFrames.
* coursesDetails
* membershipDetails
"""
def loadDatasets(courseDatasetPath, membershipDatasetPath):
    coursesDetails = pd.read_csv(courseDatasetPath)
    membershipDetails = pd.read_csv(membershipDatasetPath)
    return coursesDetails, membershipDetails

"""Fetch lessons from API
Obtiene lecciones de una API dada la URL.
"""
def fetchLessonsFromAPI(url):
    response = requests.get(url)
    data = response.json()
    df = pd.DataFrame(data)
    return df.drop(columns=['__v'])

def createDataFrameFromAPI(url=None):
    """
    Crea un DataFrame a partir de los datos obtenidos de una API.
    Si no se proporciona una URL, se toma de las variables de entorno.

    Ejecución:
        df = createDataFrameFromAPI()  Toma la URL desde COMPLETED_API_URL en .env
        df = createDataFrameFromAPI("https://api.ejemplo.com/otros_datos") Usa la URL proporcionada
    """
    # Cargar variables de entorno
    load_dotenv()
    # Si no se proporciona una URL como parámetro, se toma del .env
    if url is None:
        url = os.getenv("COMPLETED_API_URL")
        if not url:
            raise ValueError("No se proporcionó una URL y no se encontró COMPLETED_API_URL en el .env")
    # Hacer la petición a la API
    response = requests.get(url)
    # Lanza un error si la respuesta es 4xx o 5xx
    response.raise_for_status()
    # Asumimos que la respuesta es JSON
    data = response.json()
    # Convertir los datos JSON en un DataFrame
    df = pd.DataFrame(data)
    return df