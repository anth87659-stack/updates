import psycopg2
import os
from dotenv import load_dotenv
from psycopg2.extras import execute_values
import pandas as pd
from datetime import timedelta, datetime
from fetch_historico_iol import fetch_historico_iol_fast
import math
import time
import traceback

# ======================================================================
# 🎯 CONFIGURACIÓN
# ======================================================================
load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
DEFAULT_DESDE = "01/01/2020"  
LOOKBACK_DAYS = 60  # Traer últimos 60 días para asegurar actualización

# Control de rate limiting y reintentos
DELAY_BETWEEN_REQUESTS = 0.5  # segundos entre requests
MAX_RETRIES = 3
RETRY_DELAY = 2  # segundos entre reintentos

# ======================================================================
# 🛠️ PARSE NUMBER - Versión CORREGIDA
# ======================================================================
def parse_number(val, is_int=False):
    """
    Parsea números manejando múltiples formatos:
    - Formato americano: "20,390.00" (coma=miles, punto=decimal) ← IOL usa este
    - Formato argentino: "20.390,00" (punto=miles, coma=decimal)
    - Sin formato: "20390.00" o "20390"
    """
    if val is None:
        return None
    if isinstance(val, (int, float)):
        return int(val) if is_int else float(val)
    
    s = str(val).strip()
    if s == "" or s == "-":
        return None
    
    try:
        coma_pos = s.rfind(',')
        punto_pos = s.rfind('.')
        
        # Caso 1: Solo comas → "20,390" (separador de miles)
        if ',' in s and '.' not in s:
            s = s.replace(',', '')
        
        # Caso 2: Solo puntos
        elif '.' in s and ',' not in s:
            # Múltiples puntos = separadores de miles
            if s.count('.') > 1:
                s = s.replace('.', '')
            # Un solo punto: verificar si es separador decimal o de miles
            elif punto_pos > 0:
                decimales_despues = len(s) - punto_pos - 1
                # Si hay más de 3 dígitos después del punto, es separador de miles
                if decimales_despues > 3:
                    s = s.replace('.', '')
                # Si 1-3 dígitos, es decimal: mantener el punto
        
        # Caso 3: Tiene AMBOS coma y punto
        elif ',' in s and '.' in s:
            # El que está más a la derecha es el separador decimal
            if coma_pos > punto_pos:
                # Formato argentino: "20.390,50" → punto=miles, coma=decimal
                s = s.replace('.', '').replace(',', '.')
            else:
                # Formato americano: "20,390.50" → coma=miles, punto=decimal
                s = s.replace(',', '')
        
        # Convertir a número
        num = float(s)
        
        # Validación de rango razonable
        if num < 0:
            return None
        
        return int(num) if is_int else num
        
    except Exception as e:
        # Log silencioso para no spamear, pero devolver None
        return None

# ======================================================================
# 💾 UPSERT SELECTIVO - Versión OPTIMIZADA
# ======================================================================
def upsert_historico_selectivo(cur, ticker_id, ticker, historico_df):
    """
    Inserta o actualiza solo filas nuevas o cuyo contenido cambió.
    Devuelve (n_inserts_or_updates, n_skipped, fecha_mas_reciente).
    """
    if historico_df is None or historico_df.empty:
        return 0, 0, None

    def col(name):
        for c in historico_df.columns:
            if name.lower() in c.lower():
                return c
        return None

    fecha_col = col("fecha")
    apertura_col = col("apertura")
    maximo_col = col("maximo")
    minimo_col = col("minimo")
    cierre_col = col("cierre")
    volumen_col = col("volumen") or col("volumen_nominal")

    if not fecha_col:
        return 0, 0, None

    df = historico_df.copy()
    
    # CRÍTICO: Parsear fechas con detección automática (IOL usa MM/DD/YYYY)
    df[fecha_col] = pd.to_datetime(df[fecha_col], errors='coerce')
    
    # Filtrar fechas inválidas
    df = df[df[fecha_col].notna()].copy()
    
    if df.empty:
        return 0, 0, None
    
    df[fecha_col] = df[fecha_col].dt.date
    fechas = sorted(df[fecha_col].unique().tolist())
    
    if not fechas:
        return 0, 0, None
    
    fecha_mas_reciente = max(fechas)

    # Obtener registros existentes en BD
    cur.execute("""
        SELECT fecha, apertura, maximo, minimo, cierre, volumen
        FROM historicos
        WHERE ticker_id = %s AND fecha = ANY(%s)
    """, (ticker_id, fechas))
    
    existing = cur.fetchall() 
    existing_map = {
        row[0]: {
            "apertura": row[1], 
            "maximo": row[2], 
            "minimo": row[3], 
            "cierre": row[4], 
            "volumen": row[5]
        } for row in existing
    }

    records_to_write = []
    skipped = 0

    for _, row in df.iterrows():
        try:
            fecha = row[fecha_col]
            if pd.isna(fecha):
                continue

            apertura = parse_number(row[apertura_col]) if apertura_col and row.get(apertura_col) is not None else None
            maximo = parse_number(row[maximo_col]) if maximo_col and row.get(maximo_col) is not None else None
            minimo = parse_number(row[minimo_col]) if minimo_col and row.get(minimo_col) is not None else None
            cierre = parse_number(row[cierre_col]) if cierre_col and row.get(cierre_col) is not None else None
            volumen = parse_number(row[volumen_col], is_int=True) if volumen_col and row.get(volumen_col) is not None else None

            # Validación básica: saltar registros con precios sospechosamente bajos
            if cierre is not None and cierre < 0.01:
                continue

            existing_row = existing_map.get(fecha)

            if existing_row is None:
                # Fila nueva → insertar
                records_to_write.append((ticker_id, fecha, apertura, maximo, minimo, cierre, volumen))
            else:
                # Fila existente → comparar valores
                def diff(a, b):
                    if a is None and b is None:
                        return False
                    if a is None or b is None:
                        return True
                    try:
                        if isinstance(a, float) or isinstance(b, float):
                            return not math.isclose(float(a), float(b), rel_tol=1e-9, abs_tol=1e-9)
                        else:
                            return a != b
                    except Exception:
                        return a != b

                if (diff(apertura, existing_row["apertura"]) or 
                    diff(maximo, existing_row["maximo"]) or 
                    diff(minimo, existing_row["minimo"]) or 
                    diff(cierre, existing_row["cierre"]) or 
                    diff(volumen, existing_row["volumen"])):
                    records_to_write.append((ticker_id, fecha, apertura, maximo, minimo, cierre, volumen))
                else:
                    skipped += 1
                    
        except Exception as e:
            # Log silencioso: no spamear con errores de filas individuales
            continue

    if not records_to_write:
        return 0, skipped, fecha_mas_reciente

    sql = """
        INSERT INTO historicos (ticker_id, fecha, apertura, maximo, minimo, cierre, volumen)
        VALUES %s
        ON CONFLICT (ticker_id, fecha) DO UPDATE
        SET apertura = EXCLUDED.apertura,
            maximo   = EXCLUDED.maximo,
            minimo   = EXCLUDED.minimo,
            cierre   = EXCLUDED.cierre,
            volumen  = EXCLUDED.volumen
        WHERE
            historicos.apertura IS DISTINCT FROM EXCLUDED.apertura OR
            historicos.maximo   IS DISTINCT FROM EXCLUDED.maximo   OR
            historicos.minimo   IS DISTINCT FROM EXCLUDED.minimo   OR
            historicos.cierre   IS DISTINCT FROM EXCLUDED.cierre   OR
            historicos.volumen  IS DISTINCT FROM EXCLUDED.volumen;
    """

    try:
        execute_values(cur, sql, records_to_write)
    except Exception as e:
        print(f"\n   ⚠️ Error SQL para {ticker}: {str(e)[:100]}")
        raise
    
    return len(records_to_write), skipped, fecha_mas_reciente

# ======================================================================
# 🚀 MAIN - Versión PRODUCCIÓN
# ======================================================================
def main(ticker_especifico=None, limite=None):
    """
    Actualiza históricos de todos los tickers o de un subconjunto específico.
    
    Args:
        ticker_especifico: Ticker individual para actualizar (ej: "AAPL")
        limite: Número máximo de tickers a procesar (para pruebas)
    """
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()

    # Construir query según parámetros
    if ticker_especifico:
        cur.execute("""
            SELECT ticker, id 
            FROM tickers 
            WHERE ticker = %s AND tipo IN ('Cedears', 'Acciones', 'Bonos', 'Letras', 'Obligaciones Negociables')
            LIMIT 1;
        """, (ticker_especifico,))
        tickers = cur.fetchall()
        if not tickers:
            print(f"❌ No se encontró el ticker '{ticker_especifico}' o no es del tipo correcto")
            cur.close()
            conn.close()
            return
    else:
        query = """
            SELECT ticker, id 
            FROM tickers 
            WHERE tipo IN ('Cedears', 'Acciones', 'Bonos', 'Letras', 'Obligaciones Negociables')
            ORDER BY ticker
        """
        if limite:
            query += f" LIMIT {limite}"
        query += ";"
        
        cur.execute(query)
        tickers = cur.fetchall()
    
    total = len(tickers)
    inicio = datetime.now()
    
    # Header
    print("="*80)
    print("📊 ACTUALIZACIÓN DE HISTÓRICOS - IOL → PostgreSQL")
    print("="*80)
    print(f"📅 Fecha: {inicio.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"🎯 Tickers a procesar: {total:,}")
    print(f"⏱️  Delay entre requests: {DELAY_BETWEEN_REQUESTS}s")
    print(f"🔄 Reintentos por ticker: {MAX_RETRIES}")
    print(f"📆 Lookback: {LOOKBACK_DAYS} días")
    print("="*80 + "\n")

    # Estadísticas
    stats = {
        'actualizados': 0,
        'sin_cambios': 0,
        'errores': 0,
        'sin_datos': 0,
        'total_registros_nuevos': 0,
        'total_registros_skip': 0
    }

    # Procesar cada ticker
    for i, (ticker, ticker_id) in enumerate(tickers, 1):
        # Mostrar progreso resumido cada 50 tickers
        if i % 50 == 0 or i == 1:
            if i > 1:  # No mostrar en la primera iteración
                elapsed = (datetime.now() - inicio).total_seconds()
                rate = i / elapsed if elapsed > 0 else 0
                eta_seconds = (total - i) / rate if rate > 0 else 0
                eta_str = str(timedelta(seconds=int(eta_seconds)))
                
                print(f"\n{'─'*80}")
                print(f"📊 Progreso: {i-1}/{total} ({(i-1)/total*100:.1f}%) | "
                      f"Rate: {rate:.2f} ticker/s | ETA: {eta_str}")
                print(f"   ✅ {stats['actualizados']} actualizados | "
                      f"ℹ️  {stats['sin_cambios']} sin cambios | "
                      f"⚠️  {stats['sin_datos']} sin datos | "
                      f"❌ {stats['errores']} errores")
                print(f"   💾 {stats['total_registros_nuevos']:,} registros nuevos insertados")
                print(f"{'─'*80}\n")

        try:
            # Output compacto: ticker en la misma línea
            print(f"[{i}/{total}] {ticker:8s}", end=" ", flush=True)

            # Obtener última fecha en BD
            cur.execute("SELECT MAX(fecha) FROM historicos WHERE ticker_id = %s", (ticker_id,))
            last = cur.fetchone()[0]
            
            # Calcular fecha desde
            if last is None:
                desde = DEFAULT_DESDE
            else:
                # Traer últimos LOOKBACK_DAYS para asegurar captura de datos nuevos
                desde_date = max(
                    last - timedelta(days=LOOKBACK_DAYS),
                    datetime.strptime(DEFAULT_DESDE, "%d/%m/%Y").date()
                )
                desde = desde_date.strftime("%d/%m/%Y")

            # Fetch con reintentos automáticos
            df = None
            last_error = None
            
            for intento in range(MAX_RETRIES):
                try:
                    df = fetch_historico_iol_fast(
                        ticker=ticker,
                        desde=desde,
                        mercado="BCBA",
                        guardar_csv=False
                    )
                    break  # Éxito, salir del loop de reintentos
                    
                except Exception as e:
                    last_error = e
                    if intento < MAX_RETRIES - 1:
                        print(f"↻", end="", flush=True)  # Indicador de reintento
                        time.sleep(RETRY_DELAY)
                    else:
                        raise last_error

            # Verificar si hay datos
            if df is None or df.empty:
                print("⚠️ sin datos")
                stats['sin_datos'] += 1
                time.sleep(DELAY_BETWEEN_REQUESTS)
                continue

            # Insertar/actualizar en BD
            n_written, n_skipped, fecha_nueva = upsert_historico_selectivo(
                cur, ticker_id, ticker, df
            )
            conn.commit()
            
            # Output según resultado
            if n_written > 0:
                print(f"✅ +{n_written}", end="")
                stats['actualizados'] += 1
                stats['total_registros_nuevos'] += n_written
                
                # Mostrar nueva fecha más reciente si cambió
                if fecha_nueva and (last is None or fecha_nueva > last):
                    dias_nuevos = (fecha_nueva - last).days if last else None
                    if dias_nuevos and dias_nuevos > 0:
                        print(f" ({dias_nuevos}d)", end="")
                
                if n_skipped > 0:
                    print(f" [{n_skipped} skip]", end="")
            else:
                print("ℹ️  ok", end="")
                stats['sin_cambios'] += 1
                if n_skipped > 0:
                    print(f" ({n_skipped} skip)", end="")
            
            print()  # Nueva línea
            stats['total_registros_skip'] += n_skipped
            
            # Rate limiting
            time.sleep(DELAY_BETWEEN_REQUESTS)

        except KeyboardInterrupt:
            print("\n\n⚠️  Interrupción manual detectada")
            break
            
        except Exception as e:
            print(f"❌ {str(e)[:50]}")
            stats['errores'] += 1
            conn.rollback()
            time.sleep(DELAY_BETWEEN_REQUESTS * 2)  # Esperar más tras error

    # Cerrar conexión
    cur.close()
    conn.close()
    
    # Resumen final
    fin = datetime.now()
    duracion = fin - inicio
    
    print("\n" + "="*80)
    print("📊 RESUMEN FINAL")
    print("="*80)
    print(f"✅ Actualizados:         {stats['actualizados']:,} tickers")
    print(f"ℹ️  Sin cambios:          {stats['sin_cambios']:,} tickers")
    print(f"⚠️  Sin datos en IOL:     {stats['sin_datos']:,} tickers")
    print(f"❌ Errores:              {stats['errores']:,} tickers")
    print(f"📈 Total procesados:     {total:,} tickers")
    print(f"💾 Registros insertados: {stats['total_registros_nuevos']:,}")
    print(f"📋 Registros sin cambio: {stats['total_registros_skip']:,}")
    print(f"⏱️  Duración total:       {duracion}")
    
    if duracion.total_seconds() > 0:
        rate = total / duracion.total_seconds()
        print(f"📊 Rate promedio:        {rate:.2f} tickers/segundo")
    
    print("="*80)
    
    # Mensaje final según resultados
    if stats['errores'] == 0:
        print("\n✅ Actualización completada sin errores")
    elif stats['errores'] < total * 0.05:  # Menos del 5% de errores
        print(f"\n⚠️  Actualización completada con {stats['errores']} errores menores")
    else:
        print(f"\n❌ Actualización completada con {stats['errores']} errores ({stats['errores']/total*100:.1f}%)")

# ======================================================================
# 🎯 EJECUCIÓN
# ======================================================================
if __name__ == "__main__":
    import sys
    
    ticker = None
    limite = None
    
    # Parsear argumentos de línea de comandos
    if len(sys.argv) > 1:
        arg = sys.argv[1].upper()
        
        if arg.isdigit():
            # Es un número → límite de tickers
            limite = int(arg)
            print(f"🧪 MODO PRUEBA: Procesando primeros {limite} tickers\n")
        else:
            # Es un ticker específico
            ticker = arg
            print(f"🧪 MODO PRUEBA: Procesando solo {ticker}\n")
    
    try:
        main(ticker_especifico=ticker, limite=limite)
    except KeyboardInterrupt:
        print("\n\n⚠️  Proceso interrumpido por el usuario")
    except Exception as e:
        print(f"\n\n❌ Error fatal: {e}")
        traceback.print_exc()