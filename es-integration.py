import configparser
import csv
import io
from datetime import datetime
from hashlib import sha1

import pandas as pd
import requests
from elasticsearch import Elasticsearch

# Define dataset url and the corresponding ES index
CSV_URL = "https://opendata-ajuntament.barcelona.cat/data/dataset/4f3ffbda-d5be-4f2a-a836-26a77be6df1a/resource/f627ac0a-d05f-416d-9773-eeb464a3fc44/download"
es_index = "python-upload"  # TODO: proper index name

# Load dataset into pandas df
with requests.Session() as s:
    download = s.get(CSV_URL)
    decoded_content = download.content.decode("utf-8")
    cr = csv.reader(decoded_content.splitlines(), delimiter=",")

df = pd.read_csv(io.StringIO(decoded_content), low_memory=False)

# Filter dataset by current date and drop NaN
current_date = datetime.today().strftime("%Y-%m-%d")  # TODO: filter by date > last ES upload date
current_df = df[df["Data_Indicador"] == current_date].dropna()

# Prepare payload from dict keys
payload = dict.fromkeys(list(current_df.columns))

# Authenticate to ES
config = configparser.ConfigParser()
config.read("es-token.ini")
es = Elasticsearch(
    cloud_id=config["ELASTIC"]["cloud_id"],
    basic_auth=(config["ELASTIC"]["user"], config["ELASTIC"]["password"]),
)
es.info()

# Fill payload with df information and push to ES
for ii, row in current_df.iterrows():
    for index in payload.keys():
        payload[index] = row[index]
    print(payload)
    unique_id = (
        payload["Data_Indicador"]
        + "/"
        + payload["Nom_Indicador"]
        + "/"
        + payload["Nom_Variable"]
    ).encode("utf-8")
    unique_id = sha1(unique_id).hexdigest()
    es.index(index=es_index, document=payload, id=unique_id)

# Refresh index
es.indices.refresh(index=es_index)

# TODO: Check ES data first
# result = es.search(index=es_index, size=20)

# print("Got %d Hits:" % result["hits"]["total"]["value"])
# for hit in result["hits"]["hits"]:
#     print(hit["_source"])