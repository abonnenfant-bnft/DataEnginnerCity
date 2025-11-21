import json
from datetime import datetime, date

import duckdb
import pandas as pd

today_date = datetime.now().strftime("%Y-%m-%d")

def create_consolidate_tables():
    con = duckdb.connect(database = "data/duckdb/mobility_analysis.duckdb", read_only = False)
    with open("data/sql_statements/create_consolidate_tables.sql") as fd:
        statements = fd.read()
        for statement in statements.split(";"):
            print(statement)
            con.execute(statement)

def consolidate_paris_data():

    con = duckdb.connect(database = "data/duckdb/mobility_analysis.duckdb", read_only = False)
    data = {}

    with open(f"data/raw_data/{today_date}/paris_realtime_bicycle_data.json") as fd:
        data = json.load(fd)

    raw_data_df = pd.json_normalize(data)
    raw_data_df["nb_inhabitants"] = None

    city_data_df = raw_data_df[[
        "code_insee_commune",
        "nom_arrondissement_communes",
        "nb_inhabitants"
    ]]
    city_data_df.rename(columns={
        "code_insee_commune": "id",
        "nom_arrondissement_communes": "name"
    }, inplace=True)
    city_data_df.drop_duplicates(inplace = True)

    city_data_df["created_date"] = date.today()
    print(city_data_df)
    con.execute("delete from consolidate_city;")
    con.execute("INSERT OR REPLACE INTO CONSOLIDATE_CITY SELECT * FROM city_data_df;")
    with open(f"data/raw_data/{today_date}/french_communes_data.json", "r", encoding="utf-8") as fd:
        french_communes_data = json.load(fd)

    communes_df = pd.json_normalize(french_communes_data)
    enriched_df = city_data_df.merge(
        communes_df[["code", "population"]],
        how="left",
        left_on="id",
        right_on="code"
    )

    enriched_df["nb_inhabitants"] = enriched_df["population"]
    enriched_df.drop(columns=["code", "population"], inplace=True)

    enriched_df["created_date"] = date.today()

       # --- Insertion dans la table DuckDB ---
    con.register("city_data_df", enriched_df)
    con.execute("INSERT OR REPLACE INTO CONSOLIDATE_CITY SELECT * FROM city_data_df;")
    con.close()

    print("✅ Données de ville consolidées avec population.")

    
def consolidate_station_paris_data():
    con = duckdb.connect(database="data/duckdb/mobility_analysis.duckdb", read_only=False)

    with open(f"data/raw_data/{today_date}/paris_realtime_bicycle_data.json") as fd:
        data = json.load(fd)

    raw_data_df = pd.json_normalize(data)

    station_data_df = pd.DataFrame({
        "id": raw_data_df["stationcode"],
        "code": raw_data_df["stationcode"],
        "name": raw_data_df["name"],
        "city_name": raw_data_df["nom_arrondissement_communes"],
        "city_code": raw_data_df["code_insee_commune"],
        "address": None,
        "longitude": raw_data_df["coordonnees_geo.lon"],
        "latitude": raw_data_df["coordonnees_geo.lat"],
        "status": raw_data_df["is_renting"],
        "created_date": date.today(),
        "capacity": raw_data_df["capacity"]
    })

    con.register("station_data_df", station_data_df)
    con.execute("INSERT OR REPLACE INTO CONSOLIDATE_STATION SELECT * FROM station_data_df;")
    con.close()
    print("✅ Données de station consolidées.")



def consolidate_station_statement_paris_data():
    con = duckdb.connect(database="data/duckdb/mobility_analysis.duckdb", read_only=False)

    with open(f"data/raw_data/{today_date}/paris_realtime_bicycle_data.json") as fd:
        data = json.load(fd)

    raw_data_df = pd.json_normalize(data)

    statement_data_df = pd.DataFrame({
        "station_id": raw_data_df["stationcode"],
        "bicycle_docks_available": raw_data_df["numdocksavailable"],
        "bicycle_available": raw_data_df["numbikesavailable"],
        "last_statement_date": pd.to_datetime(raw_data_df["duedate"]),
        "created_date": date.today()
    })

    con.register("statement_data_df", statement_data_df)
    
    con.execute("INSERT OR REPLACE INTO CONSOLIDATE_STATION_STATEMENT SELECT * FROM statement_data_df;")
    con.close()
    print("✅ Données d’état des stations consolidées.")


def consolidate_nantes_data():
    con = duckdb.connect(database="data/duckdb/mobility_analysis.duckdb", read_only=False)

    with open(f"data/raw_data/{today_date}/nantes_realtime_bicycle_data.json") as fd:
        data = json.load(fd)

    raw_df = pd.json_normalize(data["results"])

    # Nantes = 44109 (code INSEE) ou 44000 selon ton choix
    city_data_df = pd.DataFrame([{
        "id": "44109",
        "name": "Nantes",
        "nb_inhabitants": None,
        "created_date": date.today()
    }])


    # Enrichissement si fichier communes FR dispo
    try:
        with open(f"data/raw_data/{today_date}/french_communes_data.json", "r", encoding="utf-8") as fd:
            communes = json.load(fd)

        communes_df = pd.json_normalize(communes)

        enriched_df = city_data_df.merge(
            communes_df[["code", "population"]],
            how="left",
            left_on="id",
            right_on="code"
        )

        enriched_df["nb_inhabitants"] = enriched_df["population"]
        enriched_df.drop(columns=["code", "population"], inplace=True)

        enriched_df["created_date"] = date.today()

        con.register("city_data_df", enriched_df)
        con.execute("INSERT OR REPLACE INTO CONSOLIDATE_CITY SELECT * FROM city_data_df;")

    except FileNotFoundError:
        print("⚠️ Pas de fichier french_communes_data.json : pas d’enrichissement population.")

    con.close()
    print("✅ Données ville Nantes consolidées.")

def consolidate_station_nantes_data():
    con = duckdb.connect(database="data/duckdb/mobility_analysis.duckdb", read_only=False)

    with open(f"data/raw_data/{today_date}/nantes_realtime_bicycle_data.json") as fd:
        data = json.load(fd)

    df = pd.json_normalize(data["results"])

    station_df = pd.DataFrame({
        "id": df["number"],
        "code": df["number"],
        "name": df["name"],
        "city_name": df["contract_name"],
        "city_code": "44109",  # Nantes
        "address": df["address"],
        "longitude": df["position.lon"],
        "latitude": df["position.lat"],
        "status": df["status"],
        "created_date": date.today(),
        "capacity": df["bike_stands"]
    })

    con.register("station_data_df", station_df)
    con.execute("INSERT OR REPLACE INTO CONSOLIDATE_STATION SELECT * FROM station_data_df;")
    con.close()

    print("✅ Données stations Nantes consolidées.")
def consolidate_station_statement_nantes_data():
    con = duckdb.connect(database="data/duckdb/mobility_analysis.duckdb", read_only=False)

    with open(f"data/raw_data/{today_date}/nantes_realtime_bicycle_data.json") as fd:
        data = json.load(fd)

    df = pd.json_normalize(data["results"])

    statement_df = pd.DataFrame({
        "station_id": df["number"],
        "bicycle_docks_available": df["available_bike_stands"],
        "bicycle_available": df["available_bikes"],
        "last_statement_date": pd.to_datetime(df["last_update"]),
        "created_date": date.today()
    })

    con.register("statement_data_df", statement_df)
    con.execute("INSERT OR REPLACE INTO CONSOLIDATE_STATION_STATEMENT SELECT * FROM statement_data_df;")
    con.close()

    print("✅ Données d’état des stations Nantes consolidées.")

