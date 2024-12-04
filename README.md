# Dashboard
Dashboard interactivo con manejo de archivos y ETL - Python

## Estructura de carpetas
```bash
dashboard-interactivo/
├── .env                # Variables de entorno
├── .gitignore          # Archivos que serán ignorados por Git
├── data/               # Carpeta para almacenar los archivos CSV
│   ├── .gitkeep        # Mantiene la carpeta en el repositorio (los CSV se ignorarán)
├── src/                # Código fuente del proyecto
│   ├── etl/            # Módulo ETL
│   │   ├── __init__.py # Indica que se trata de un módulo
│   │   ├── extract.py  # Extracción de datos desde las APIs
│   │   ├── transform.py # Transformación de datos
│   │   ├── load.py      # Carga de datos a CSV
│   ├── dashboard/      # Código del dashboard
│   │   ├── app.py      # Archivo principal para Dash
│   │   ├── utils.py    # Utilidades compartidas (e.g., funciones para manejo de CSV)
├── tests/              # Pruebas unitarias para cada módulo
│   ├── test_etl.py     # Pruebas del módulo ETL
│   ├── test_dashboard.py # Pruebas del dashboard
├── requirements.txt    # Dependencias del proyecto
├── README.md           # Descripción del proyecto
```