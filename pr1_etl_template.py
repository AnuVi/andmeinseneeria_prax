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
API_URL = "https://restcountries.com/v3.1/region/europe?fields=name,capital,population,area"


def extract():
    """
    Extract: loe REST API-st riikide andmed.

    Tagasta JSON andmed listina.

    Näide kuidas API-st andmeid pärida:
        response = requests.get("https://mingi-api.com/andmed")
        data = response.json()  # tagastab Pythoni listi/dict'i
    """

    # TODO: päri andmed API_URL-ist ja tagasta tulemus
    
    response = requests.get(API_URL)
    raw_data = response.json()   # JSON → list of dicts
    print("Näidis andmest:", raw_data[0])  # debug print
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
        #continent = r.get("region", "Unknown")  # kui API-s on 'region' või 'continent'

        rows.append((name, capital, population, area))
    rows.sort(key=lambda r: r[2], reverse=True)
    return rows 
    pass


def load(rows=None):
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
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS europe_countries (id SERIAL PRIMARY KEY, name TEXT, capital TEXT, population INT, area_km2 FLOAT, continent TEXT, loaded_at TIMESTAMP)")
    conn.commit()
    cur.execute("""
        SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE table_name = 'europe_countries'
    );
""")
    exists = cur.fetchone()[0]

    if exists:
        print("Tabel europe_countries on olemas ✅")
    else:
        print("Tabel europe_countries puudub ❌")
    conn.commit()
     # idempotentsuse tagamiseks tühjenda tabel
    cur.execute("TRUNCATE TABLE europe_countries")
    conn.commit()

    # lisa read
    if rows:
        for row in rows:
            cur.execute("""
                INSERT INTO europe_countries (name, capital, population, area_km2, continent, loaded_at)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (*row, "Europe", datetime.now()))

    conn.commit()
    cur.close()
    conn.close()
    pass


def main():
    print("=== ETL protsess ===\n")

    # Extract
    raw = extract()
    print(f"Extracted: {len(raw)} kirjet\n")

    # Transform
    rows = transform(raw)
    print(f"Transformed: {len(rows)} rida\n")

    # Load
    load(rows)

    print("\n=== ETL lõpetatud ===")


if __name__ == "__main__":
    main()
