import pandas as pd
import uuid
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

def nan_to_none(val):
    if pd.isna(val):
        return None
    elif val=="?":
        return None
    return val

def convert_to_bool_or_none(val):
    if val in [True, 'True', 'true', '1']:
        return True
    elif val in [False, 'False', 'false', '0']:
        return False
    else:
        return None

def create_table_if_not_exists(session, keyspace, table_name):
    drop_query = f"DROP TABLE IF EXISTS {keyspace}.{table_name}"
    session.execute(drop_query)
    # Créez la table
    create_query = f"""
    CREATE TABLE {keyspace}.{table_name} (
        user_id uuid PRIMARY KEY,
        age int,
        sexe text,
        taux int,
        situationFamiliale text,
        nbEnfantsAcharge int,
        deuxiemeVoiture boolean,
        immatriculation text
    )
    """
    session.execute(create_query)
    print(f"Table {keyspace}.{table_name} created.")

def process_data(df):
    df = df.map(nan_to_none)
    df = df.map(lambda val: None if pd.isna(val) else val)
    df['age'] = pd.to_numeric(df['age'], errors='coerce').fillna(0).astype(int)
    df['taux'] = pd.to_numeric(df['taux'], errors='coerce').fillna(0).astype(int)
    df['nbEnfantsAcharge'] = pd.to_numeric(df['nbEnfantsAcharge'], errors='coerce').fillna(0).astype(int)
    df['deuxiemeVoiture'] = df['deuxiemeVoiture'].apply(convert_to_bool_or_none)
    df.insert(0, 'user_id', [uuid.uuid4() for _ in range(len(df))])
    return df

def import_csv_to_scylladb(csv_file_path, keyspace, table_name, node_address, username, password):
    try:
        print("Connecting to ScyllaDB cluster...")
        auth_provider = PlainTextAuthProvider(username=username, password=password)
        cluster = Cluster([node_address], auth_provider=auth_provider)
        session = cluster.connect(keyspace)

        create_table_if_not_exists(session, keyspace, table_name)

        print(f"Loading data from CSV file: {csv_file_path}")
        df = pd.read_csv(csv_file_path, encoding='ISO-8859-1')
        df = process_data(df)

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
csv_file = '../data/Clients_19.csv'
import_csv_to_scylladb(csv_file, 'tpaprojet', 'clients', '127.0.0.1', 'username', 'password')

