import psycopg2
import os
from dotenv import load_dotenv
from psycopg2.extras import execute_values
import pandas as pd
from datetime import timedelta
from fetch_historico_iol import fetch_historico_iol_fast
import math
import traceback

# ======================================================================
# CONFIG
# ======================================================================
load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
DEFAULT_DESDE = "01/01/2020"  # uso si no hay históricos previos

# ======================================================================
# UTIL: normalizar valores para comparar
# ======================================================================
def parse_number(val, is_int=False):
    if val is None:
        return None
    if isinstance(val, (int, float)):
        return int(val) if is_int else float(val)
    s = str(val).strip()
    if s == "":
        return None
    try:
        if "," in s and "." in s:
            s = s.replace(".", "").replace(",", ".")
        else:
    
            if "," in s and "." not in s:
                s = s.replace(",", ".")
            else:
                pass
        if is_int:
            return int(float(s))
        return float(s)
    except Exception:
        s2 = s.replace(",", "").replace(".", "")
        try:
            return int(s2) if is_int else float(s2)
        except Exception:
            return None

# ======================================================================
# UPSERT selectivo: solo filas nuevas o cambiadas
# ======================================================================
def upsert_historico_selectivo(cur, ticker_id, historico_df):
    """Inserta o actualiza solo filas que son nuevas o cuyo contenido cambió.
       Devuelve (n_inserts_or_updates, n_skipped).
    """
    if historico_df is None or historico_df.empty:
        return 0, 0

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
        print("⚠️ No se encontró columna de fecha")
        return 0, 0

    df = historico_df.copy()
    df[fecha_col] = pd.to_datetime(df[fecha_col]).dt.date

    fechas = sorted(df[fecha_col].dropna().unique().tolist())
    if not fechas:
        return 0, 0

    cur.execute("""
        SELECT fecha, apertura, maximo, minimo, cierre, volumen
        FROM historicos
        WHERE ticker_id = %s AND fecha = ANY(%s)
    """, (ticker_id, fechas))
    existing = cur.fetchall() 
    existing_map = {row[0]: {"apertura": row[1], "maximo": row[2], "minimo": row[3], "cierre": row[4], "volumen": row[5]} for row in existing}

    records_to_write = []
    skipped = 0
    errors = 0

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

            existing_row = existing_map.get(fecha)

            if existing_row is None:
                # fila nueva -> insertar
                records_to_write.append((ticker_id, fecha, apertura, maximo, minimo, cierre, volumen))
            else:
              
                def diff(a, b):
                    # a: valor nuevo (float/int/None), b: existente (float/int/None)
                    if a is None and b is None:
                        return False
                    if a is None and b is not None:
                        return True
                    if a is not None and b is None:
                        return True
                    # ambos no None -> comparar numéricamente con tolerancia si float
                    try:
                        # si b viene como Decimal o int/float
                        if isinstance(a, float) or isinstance(b, float):
                            return not math.isclose(float(a), float(b), rel_tol=1e-9, abs_tol=1e-9)
                        else:
                            return a != b
                    except Exception:
                        return a != b

                if diff(apertura, existing_row["apertura"]) or \
                   diff(maximo, existing_row["maximo"]) or \
                   diff(minimo, existing_row["minimo"]) or \
                   diff(cierre, existing_row["cierre"]) or \
                   diff(volumen, existing_row["volumen"]):
                    records_to_write.append((ticker_id, fecha, apertura, maximo, minimo, cierre, volumen))
                else:
                    skipped += 1
        except Exception as e:
            errors += 1
            print("   ⚠️ Error parseando fila:", e)
            traceback.print_exc()

    if not records_to_write:
        return 0, skipped

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

    execute_values(cur, sql, records_to_write)
    return len(records_to_write), skipped

# ======================================================================
# MAIN
# ======================================================================
def main():
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()

    cur.execute("""
        SELECT ticker, id 
        FROM tickers 
        WHERE tipo IN ('Cedears', 'Acciones', 'Bonos', 'Letras', 'Obligaciones Negociables')
        ORDER BY ticker;
    """)
    tickers = cur.fetchall()
    total = len(tickers)
    print(f"📊 Total de tickers a actualizar: {total}\n")

    for i, (ticker, ticker_id) in enumerate(tickers, 1):
        try:
            print(f"[{i}/{total}] 🔄 {ticker} (id={ticker_id})...")

            cur.execute("SELECT MAX(fecha) FROM historicos WHERE ticker_id = %s", (ticker_id,))
            last = cur.fetchone()[0]  # puede ser None
            if last is None:
                desde = DEFAULT_DESDE
            else:
                desde_date = last + timedelta(days=1)
                desde = desde_date.strftime("%d/%m/%Y")  # formato que parece usar tu fetch

            df = fetch_historico_iol_fast(
                ticker=ticker,
                desde=desde,
                mercado="BCBA",
                guardar_csv=False
            )

            if df is None or df.empty:
                print("   ⚠️ Sin datos nuevos (fetch vacío)")
                continue

            n_written, n_skipped = upsert_historico_selectivo(cur, ticker_id, df)
            conn.commit()
            print(f"   ✅ {n_written} filas insertadas/actualizadas, {n_skipped} filas sin cambios")

        except Exception as e:
            print(f"   ❌ Error con {ticker}: {e}")
            traceback.print_exc()
            conn.rollback()

    cur.close()
    conn.close()
    print("\n✅ Actualización completada")

if __name__ == "__main__":
    main()
