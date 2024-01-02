import pandas as pd
import uuid
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

def nan_to_none(val):
    if pd.isna(val):
        return None
    elif val == "?":
        return None
    return val

def convert_to_bool_or_none(val):
    if val in [True, 'True', 'true', '1']:
        return True
    elif val in [False, 'False', 'false', '0']:
        return False
    else:
        return None

def create_immatriculations_table(session, keyspace):
    drop_query = f"DROP TABLE IF EXISTS {keyspace}.immatriculations"
    session.execute(drop_query)
    create_query = f"""
    CREATE TABLE IF NOT EXISTS {keyspace}.immatriculations (
        id uuid PRIMARY KEY,
        immatriculation text,
        marque text,
        nom text,
        puissance int,
        longueur text,
        nbPlaces int,
        nbPortes int,
        couleur text,
        occasion boolean,
        prix int
    )
    """
    session.execute(create_query)
    print(f"Table {keyspace}.immatriculations created.")

def process_immatriculations_data(df):
    df = df.applymap(nan_to_none)
    df['puissance'] = pd.to_numeric(df['puissance'], errors='coerce').fillna(0).astype(int)
    df['nbPlaces'] = pd.to_numeric(df['nbPlaces'], errors='coerce').fillna(0).astype(int)
    df['nbPortes'] = pd.to_numeric(df['nbPortes'], errors='coerce').fillna(0).astype(int)
    df['prix'] = pd.to_numeric(df['prix'], errors='coerce').fillna(0).astype(int)
    df['occasion'] = df['occasion'].apply(convert_to_bool_or_none)
    df.insert(0, 'id', [uuid.uuid4() for _ in range(len(df))])
    return df

def import_immatriculations_to_scylladb(csv_file_path, keyspace, table_name, node_address, username, password):
    try:
        print("Connecting to ScyllaDB cluster...")
        auth_provider = PlainTextAuthProvider(username=username, password=password)
        cluster = Cluster([node_address], auth_provider=auth_provider)
        session = cluster.connect(keyspace)

        create_immatriculations_table(session, keyspace)

        print(f"Loading data from CSV file: {csv_file_path}")
        df = pd.read_csv(csv_file_path, encoding='ISO-8859-1')
        df = process_immatriculations_data(df)

        print(f"Preparing to insert data into {keyspace}.{table_name}...")
        columns = ', '.join(df.columns)
        placeholders = ', '.join(['%s'] * len(df.columns))
        insert_query = f"INSERT INTO {keyspace}.{table_name} ({columns}) VALUES ({placeholders})"

        print("Inserting data...")
        for _, row in df.iterrows():
            session.execute(insert_query, tuple(row))
        print("Data insertion complete.")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        cluster.shutdown()
        print("Connection closed.")

# Exemple d'utilisation
csv_file = '../data/Immatriculations.csv'
import_immatriculations_to_scylladb(csv_file, 'tpaprojet', 'immatriculations', '127.0.0.1', 'username', 'password')

