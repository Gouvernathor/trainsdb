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

    for fold in os.listdir("db"):
        db[fold] = {}
        for file in _tables:
            fp = os.path.join("db", fold, file+".csv")
            if not os.path.exists(fp):
                fp = os.path.join("db", fold, file+".txt")
                if not os.path.exists(fp):
                    raise RuntimeError(f"Fichier {file!r} absent dans la base de données {fold!r} ({fp!r})")
                    # décider quoi faire quand un des fichiers est absent
            db[fold][file] = pd.read_csv(fp)

    # mise en forme de certaines données

    for fold, group in db.items():
        # heures
        di = group.get("stop_times")
        if di is not None:
            di["arrival_time"] = pd.to_datetime(di["arrival_time"], format="%H:%M:%S", errors="coerce").dt.time
            di["departure_time"] = pd.to_datetime(di["departure_time"], format="%H:%M:%S", errors="coerce").dt.time

        # dates
        di = group.get("calendar_dates")
        if di is not None:
            di["date"] = pd.to_datetime(di["date"], format="%Y%m%d", errors="coerce").dt.date
            # di = group["calendar"]
            # di["start_date"] = pd.to_datetime(di["start_date"], format="%Y%m%d", errors="coerce").dt.date
            # di["end_date"] = pd.to_datetime(di["end_date"], format="%Y%m%d", errors="coerce").dt.date

    return db

def merge_db(db):
    """
    Fusionne les bases de données en une seule, on fusionne les agency entre elles, les trips, etc.
    On finit avec un dictionnaire qui associe les tables aux noms de tables.
    """
    # version avec le nom de la base de données source mise en index
    # merged = {tab_id : pd.concat([d[tab_id] for d in db.values()], keys=list(db.keys())) for tab_id in _tables}

    # version avec le nom mis en colonne
    for groupname, group in db.items():
        for tab_id, table in group.items():
            table["source"] = groupname

    merged = {tab_id : pd.concat([d[tab_id] for d in db.values()], ignore_index=True, join="inner") for tab_id in _tables}

    shapes = {name : tab.shape for name, tab in merged.items()}
    for tab_id, table in merged.items():
        merged[tab_id] = table.drop_duplicates(subset=_tables[tab_id], keep="first")
        if tab_id not in ("agency",):
            # assert shapes[tab_id] == merged[tab_id].shape, f"Table {tab_id!r} : {shapes[tab_id]} -> {merged[tab_id].shape}"
            if shapes[tab_id] != merged[tab_id].shape:
                print(f"Table {tab_id!r} : {shapes[tab_id]} -> {merged[tab_id].shape}")
    return merged

db = merge_db(build())

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
