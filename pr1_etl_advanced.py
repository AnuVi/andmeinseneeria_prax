"""
ETL skripti mall: loeb REST API-st andmeid ja laeb need PostgreSQL andmebaasi.

Ülesanne: täida extract(), transform() ja load() funktsioonid.
"""

import requests
import psycopg2
import os
from datetime import datetime

# Andmebaasi ühenduse seaded (loetakse keskkonnamuutujatest)
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "db"),
    "port": int(os.getenv("DB_PORT", 5432)),
    "dbname": os.environ["POSTGRES_DB"],
    "user": os.environ["POSTGRES_USER"],
    "password": os.environ["POSTGRES_PASSWORD"],
}

# API URL Euroopa riikide andmete jaoks
API_URLS = [
    ("Europe", "https://restcountries.com/v3.1/region/europe?fields=name,capital,population,area"),
    ("Asia", "https://restcountries.com/v3.1/region/asia?fields=name,capital,population,area")
]


def extract():
    """
    Extract: loe REST API-st riikide andmed.

    Tagasta JSON andmed listina.

    Näide kuidas API-st andmeid pärida:
        response = requests.get("https://mingi-api.com/andmed")
        data = response.json()  # tagastab Pythoni listi/dict'i
    """

    # TODO: päri andmed API_URL-ist ja tagasta tulemus
    raw_data=[]
    for continent, url in API_URLS:
        response = requests.get(url)
        data = response.json()
    
        for country in data:
            country["continent"] = continent  # lisad juurde
            raw_data.append(country)
       
    
    print("Näidis andmest:estract()", raw_data[0])  # debug print
    return raw_data 

def transform(raw_data):
    """
    Transform: puhasta ja normaliseeri andmed.

    Sisend: JSON list API-st (iga element on dict)
    Väljund: list tuple'itest kujul (name, capital, population, area, continent)

    Näide kuidas JSON-ist andmeid võtta:
        item = {"name": {"common": "Estonia"}, "capital": ["Tallinn"], "population": 1331057}
        nimi = item["name"]["common"]           # -> "Estonia"
        pealinn = item["capital"][0]            # -> "Tallinn"
        rahvaarv = item["population"]           # -> 1331057

    Sorteeri tulemus rahvaarvu järgi kahanevalt:
        rows.sort(key=lambda r: r[2], reverse=True)
    """
    # TODO: käi raw_data üle, võta igast elemendist vajalikud väljad, tagasta list tuple'itest
    
    rows = []
    for r in raw_data:
        # Iga element dict → võta vajalikud väljad
        name = r["name"]["common"]  # common name
        capital = r["capital"][0] if r.get("capital") else None  # esimene kapital või None
        population = r.get("population", 0)  # default 0, kui puudu
        area = r.get("area", 0.0)            # default 0.0
        continent = r.get("continent", None)
         # arvuta rahvastikutihedus, väldi jagamist nulliga
        pop_density = round(float(population/area), 2) if area > 0 else 0.00
        

        rows.append((name, capital, population, area, continent, pop_density))
    rows.sort(key=lambda r: r[2], reverse=True) #sortimine suurimast väiksemani
    print("transform():", rows[-5:]) 
    #print(f"{pop_density:.2f}") #Float 0.0 - ülearuseid nulle ei kuvata (0.90), kui on soovi siis tuleks just nii välja printida
    return rows 
    
    
    


def load(rows, conn, cur):
    """
    Load: kirjuta andmed PostgreSQL tabelisse europe_countries.

    Tabel peab sisaldama: id, name, capital, population, area_km2, continent, loaded_at
    Laadimine peab olema idempotentne (TRUNCATE enne laadimist).

    Näide kuidas PostgreSQL-iga ühenduda ja andmeid sisestada:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        cur.execute("CREATE TABLE IF NOT EXISTS test (id SERIAL PRIMARY KEY, name TEXT)")
        cur.execute("INSERT INTO test (name) VALUES (%s)", ("väärtus",))
        conn.commit()
        cur.close()
        conn.close()
    """
    # TODO: loo tabel, tühjenda see (TRUNCATE), sisesta andmed, kinnita (commit)
    
   # conn = psycopg2.connect(**DB_CONFIG)
    #cur = conn.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS mixed_countries (id SERIAL PRIMARY KEY, name TEXT, capital TEXT, population INT, area_km2 FLOAT, continent TEXT, density FLOAT, loaded_at TIMESTAMP)")
    conn.commit()
    cur.execute("""
        SELECT EXISTS
        (
            SELECT FROM information_schema.tables 
            WHERE table_name = 'mixed_countries'
        );
    """)

    exists = cur.fetchone()[0]

    if exists:
        print("Tabel mixed_countries on olemas ✅")
    else:
        print("Tabel mixed_countries puudub ❌")
    conn.commit()
    
     # idempotentsuse tagamiseks tühjenda tabel
    cur.execute("TRUNCATE TABLE population_density_ranking, mixed_countries")
    conn.commit()

    # lisa read
    if rows:
        for row in rows:
            cur.execute("""
                 INSERT INTO mixed_countries (name, capital, population, area_km2, continent, density, loaded_at)
                 VALUES (%s, %s, %s, %s, %s, %s,%s)
                """, (*row,  datetime.now()))

    conn.commit()
    
    #ranking tabeli loomine
    cur.execute("CREATE TABLE IF NOT EXISTS population_density_ranking (id SERIAL PRIMARY KEY, country_id INT REFERENCES mixed_countries(id),ranking INT )")
    conn.commit()
    cur.execute("""
        SELECT EXISTS
        (
            SELECT FROM information_schema.tables 
            WHERE table_name = 'population_density_ranking'
        );
    """)

    exists = cur.fetchone()[0]

    if exists:
        print("Tabel population_density_ranking on olemas ✅")
    else:
        print("Tabel population_density_ranking puudub ❌")
    conn.commit()
     # idempotentsuse tagamiseks tühjenda tabel
    cur.execute("TRUNCATE TABLE population_density_ranking")
    conn.commit()

    # lisa read
    #top20 = sorted(rows, key=lambda r: r[5], reverse=True)[:20] -> viienda koha järgi
    cur.execute("""
    INSERT INTO population_density_ranking (country_id, ranking)
    SELECT id, ROW_NUMBER() OVER (ORDER BY density DESC)
    FROM mixed_countries
    ORDER BY density DESC
    LIMIT 20;
""")
    conn.commit()
    #cur.close()
    #conn.close() 



def main():
    print("=== ETL protsess ===\n")

    # Muutuja logimiseks
    start_time = datetime.now()
    rows_loaded = 0
    status = "success"
    error_message = None
    conn = None
    cur = None

    try:
        # PostgreSQL ühendus
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        # Loo etl_log tabel, kui pole olemas
        cur.execute("""
            CREATE TABLE IF NOT EXISTS etl_log (
                id SERIAL PRIMARY KEY,
                start_time TIMESTAMP,
                end_time TIMESTAMP,
                duration_seconds FLOAT,
                rows_loaded INT,
                status TEXT,
                error_message TEXT
            )
        """)
        conn.commit()

        # -----------------------------
        # Extract
        # -----------------------------
        raw = extract()
        print(f"Extracted: {len(raw)} kirjet\n")

        # -----------------------------
        # Transform
        # -----------------------------
        rows = transform(raw)
        print(f"Transformed: {len(rows)} rida\n")

        # -----------------------------
        # Load
        # -----------------------------
        load(rows, conn, cur)
        print("Load completed\n")
        rows_loaded = len(rows)

    except Exception as e:
        status = "error"
        error_message = str(e)
        print("❌ ETL error:", error_message)

    finally:
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        # Logimine etl_log tabelisse, kui ühendus olemas
        if cur is not None and conn is not None:
            try:
                cur.execute("""
                    INSERT INTO etl_log
                    (start_time, end_time, duration_seconds, rows_loaded, status, error_message)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (start_time, end_time, duration, rows_loaded, status, error_message))
                conn.commit()
            except Exception as log_err:
                print("❌ Logimine ebaõnnestus:", log_err)
            finally:
                cur.close()
                conn.close()

        print("\n=== ETL lõpetatud ===")


if __name__ == "__main__":
    main()
