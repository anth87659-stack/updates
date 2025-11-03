import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import date
import re

def get_idtitulo(ticker, mercado="BCBA", session=None):
    """
    Obtiene el idtitulo (ID interno) de un ticker accediendo a su página.
    """
    if session is None:
        session = requests.Session()
    
    url = f"https://iol.invertironline.com/Titulo/DatosHistoricos?simbolo={ticker}&mercado={mercado}"
    
    try:
        response = session.get(url, timeout=30)
        response.raise_for_status()
        
        match = re.search(r'idtitulo["\']?\s*[:=]\s*["\']?(\d+)', response.text)
        if match:
            return match.group(1)
        
        soup = BeautifulSoup(response.text, 'html.parser')
        idtitulo_input = soup.find('input', {'name': 'idtitulo'})
        if idtitulo_input and idtitulo_input.get('value'):
            return idtitulo_input.get('value')
        
        elem = soup.find(attrs={'data-idtitulo': True})
        if elem:
            return elem['data-idtitulo']
            
        print(f"⚠️ No se encontró idtitulo para {ticker}")
        return None
        
    except Exception as e:
        print(f"❌ Error al obtener idtitulo de {ticker}: {e}")
        return None


def fetch_historico_iol_fast(ticker, desde="01/01/2020", hasta=None, mercado="BCBA", guardar_csv=False):
    """
    Descarga histórico de IOL usando requests directo.
    Ahora obtiene automáticamente el idtitulo necesario.
    """
    hasta = hasta or date.today().strftime("%d/%m/%Y")
    rango = f"{desde} - {hasta}"
    
    # Crear sesión para mantener cookies
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36',
        'Accept': 'text/html, */*; q=0.01',
        'Accept-Language': 'es-ES,es;q=0.5',
        'X-Requested-With': 'XMLHttpRequest',
        'Origin': 'https://iol.invertironline.com',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
    })
    
    print(f"🕐 Descargando {ticker} desde {rango}...")
    
    # Paso 1: Obtener idtitulo
    print(f"  → Obteniendo idtitulo...")
    idtitulo = get_idtitulo(ticker, mercado, session)
    
    if not idtitulo:
        print(f"❌ No se pudo obtener idtitulo para {ticker}")
        return pd.DataFrame()
    
    print(f"  ✓ idtitulo: {idtitulo}")
    
    url = "https://iol.invertironline.com/Titulo/DatosHistoricos"
    
    session.headers.update({
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'Referer': f'https://iol.invertironline.com/Titulo/DatosHistoricos?simbolo={ticker}&mercado={mercado}'
    })
    
    payload = {
        'desdehasta': rango,
        'idtitulo': idtitulo,
        'idfrecuencias': '1',  
    }
    
    try:
        response = session.post(url, data=payload, timeout=30)
        response.raise_for_status()
        

        soup = BeautifulSoup(response.text, 'html.parser')
        table = soup.find('table', id='tbcotizaciones')
        
        if not table:
            print(f"⚠️ No se encontró tabla para {ticker}")
            return pd.DataFrame()
        
        # Extraer headers
        thead = table.find('thead')
        if not thead:
            print(f"⚠️ No se encontró thead en la tabla de {ticker}")
            return pd.DataFrame()
            
        headers_row = thead.find_all('th')
        headers = [th.get_text(strip=True) for th in headers_row]
        
        rows = []
        tbody = table.find('tbody')
        if tbody:
            for tr in tbody.find_all('tr'):
                cols = [td.get_text(strip=True) for td in tr.find_all('td')]
                if cols:  # Solo si hay datos
                    rows.append(cols)
        
        if not rows:
            print(f"⚠️ No hay datos para {ticker}")
            return pd.DataFrame()
 
        df = pd.DataFrame(rows, columns=headers)
        
        df.columns = [c.strip().replace(" ", "_").replace("á", "a").replace("é", "e").replace("í", "i").replace("ó", "o").replace("ú", "u") for c in df.columns]
        
        if guardar_csv:
            archivo = f"{ticker}_{mercado}_historico.csv"
            df.to_csv(archivo, index=False, encoding="utf-8-sig")
            print(f"📁 Archivo guardado: {archivo}")
        
        print(f"✅ {ticker}: {len(df)} filas descargadas")
        return df
        
    except requests.RequestException as e:
        print(f"❌ Error al descargar {ticker}: {e}")
        return pd.DataFrame()
