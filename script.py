import datetime
import os.path
import pandas as pd

# table -> colonne(s) d'indexation
_tables = dict(agency="agency_id",
            #    calendar="service_id",
            #    calendar_dates=None, # retiré parce que le format n'est pas le même sur toutes les bases de données, à investiguer
            #    feed_info="feed_id",
               routes="route_id",
            #    stop_extensions="object_code",
               stop_times=None,
               stops="stop_id",
               transfers=["from_stop_id", "to_stop_id"],
               trips="trip_id")

def build():
    """
    Construit la base de données à partir des fichiers CSV.
    """
    db = {}

    foldlist = os.listdir("db")

    for tab in _tables:
        tomerge = []
        for fold in foldlist:
            fp = os.path.join("db", fold, tab+".csv")
            if not os.path.exists(fp):
                fp = os.path.join("db", fold, tab+".txt")
                if not os.path.exists(fp):
                    raise RuntimeError(f"Fichier {tab!r} absent dans la base de données {fold!r} ({fp!r})")
                    # décider quoi faire quand un des fichiers est absent
            tomerge.append(pd.read_csv(fp))
            tomerge[-1]["source"] = fold
        db[tab] = table = pd.concat(tomerge, ignore_index=True, join="inner")
        # pour avoir le nom de la base de données en index, keys=foldlist

        # mise en forme de certaines données
        # heures
        if tab == "stop_times":
            table["arrival_time"] = pd.to_datetime(table["arrival_time"], format="%H:%M:%S", errors="coerce").dt.time
            table["departure_time"] = pd.to_datetime(table["departure_time"], format="%H:%M:%S", errors="coerce").dt.time

        # dates
        elif tab == "calendar_dates":
            table["date"] = pd.to_datetime(table["date"], format="%Y%m%d", errors="coerce").dt.date

        elif tab == "calendar":
            table["start_date"] = pd.to_datetime(table["start_date"], format="%Y%m%d", errors="coerce").dt.date
            table["end_date"] = pd.to_datetime(table["end_date"], format="%Y%m%d", errors="coerce").dt.date

        # suppression des doublons
        shape = table.shape
        table.drop_duplicates(subset=_tables[tab], keep="first", inplace=True, ignore_index=True)

        if shape != table.shape:
            print(f"Table {tab!r} : {shape} -> {table.shape}")

    return db

db = build()

## premières requêtes

# le nombre de trains qui passent par Versailles Chantiers entre 11h et midi
def req1():
    # trouver les identifiants des arrêts de Versailles Chantiers - attention, il y en a plusieurs
    stops = db["stops"]
    stop_ids = stops[stops["stop_name"] == "Versailles Chantiers"]["stop_id"]
    # stop_ids est une liste, ou plutôt une Series, de plusieurs valeurs qui sont des identifiants correspondant à Versailles Chantiers
    # return stop_ids
    # on peut donc faire une requête sur stop_times pour trouver les trains qui passent par ces arrêts
    stop_times = db["stop_times"]
    trips = stop_times[stop_times["stop_id"].isin(stop_ids)]
    # attention, dans ce cas, trips est un DataFrame, qui correspond à un tableau en deux dimensions, au lieu d'une pour les Series
    # on peut filtrer les horaires, pour ça on va créer des objets time
    frm = datetime.time(11, 0) # 11h
    to = datetime.time(12, 0) # midi
    trip_ids = trips[(frm <= trips["departure_time"]) & (trips["departure_time"] <= to)]["trip_id"]
    # comme on indexe à la fin sur une seule colonne, "trip_ids", on obtient une Series en une seule dimension
    return trip_ids
