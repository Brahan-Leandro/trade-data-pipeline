import pandas as pd
from googleapiclient.discovery import build
from google.oauth2 import service_account
import os
from dotenv import load_dotenv

def get_google_sheets_data(SPREADSHEET_ID, SHEET_NAMES, creds):
    """
    Obtiene los datos de las hojas de cálculo de Google y los devuelve como un diccionario de DataFrames.
    """
    service = build('sheets', 'v4', credentials=creds)
    sheet = service.spreadsheets()
    dataframes = {}

    for name in SHEET_NAMES:
        result = sheet.values().get(spreadsheetId=SPREADSHEET_ID, range=name).execute()
        values = result.get('values', [])
        
        if values:
            dataframes[name] = pd.DataFrame(values[1:], columns=values[0])
        else:
            print(f'La hoja {name} está vacía')

    return dataframes

def extract_data_from_google_sheets():
    """
    Extrae los datos de las hojas de cálculo de Google y los devuelve como un diccionario de DataFrames.
    """
    load_dotenv()
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
    KEY = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID")
    SHEET_NAMES = ['categorias', 'pedidos', 'empleados', 'clientes', 'pedidos_detalles', 'productos', 'transportistas']
    
    try:
        creds = service_account.Credentials.from_service_account_file(KEY, scopes=SCOPES)
        dataframes = get_google_sheets_data(SPREADSHEET_ID, SHEET_NAMES, creds)
        print("Proceso de extracción de datos de GSheets exitoso")
        return dataframes
    except Exception as e:
        print(f"Error al extraer datos de las hojas de cálculo de Google: {str(e)}")
        return None
