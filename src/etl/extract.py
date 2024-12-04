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