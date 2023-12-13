# Importa le librerie di Spark necessarie
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, StructType, StructField
import requests
import re
import time
from json.decoder import JSONDecodeError

# Crea una sessione Spark
spark = SparkSession.builder.appName("demographic_enrichment").getOrCreate()

# Definisci il mapping dei generi
genre_mapping = {
    'Q6581097': 'Male',
    'Q6581072': 'Female',
    'Q1097630': 'Non specificato'
}

# Funzione per ottenere i dati demografici
def get_demographic_data(artist_name):
    if artist_name is None or not isinstance(artist_name, str):
        return (None, None, None)

    cleaned_artist_name = re.sub(r'[^\w\s&,]+', '', artist_name)

    if '&' in cleaned_artist_name:
        cleaned_artist_name = cleaned_artist_name.split('&')[0].strip()
    elif 'and' in cleaned_artist_name:
        cleaned_artist_name = cleaned_artist_name.split('and')[0].strip()

    sparql_query = f"""
    SELECT ?genre ?birthDate ?birthPlaceLabel
    WHERE {{
      ?artist rdfs:label "{cleaned_artist_name}"@en.
      ?artist wdt:P21 ?genre;
              wdt:P569 ?birthDate;
              wdt:P19 ?birthPlace.

      SERVICE wikibase:label {{ bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }}
    }}
    LIMIT 1
    """
    endpoint_url = "https://query.wikidata.org/sparql"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'application/json',
    }

    max_retries = 3
    for attempt in range(max_retries):
        try:
            response = requests.get(endpoint_url, headers=headers, params={'query': sparql_query, 'format': 'json'}, timeout=120)
            response.raise_for_status()
            data = response.json()
            break
        except requests.exceptions.Timeout:
            time.sleep(5)
            if attempt == max_retries - 1:
                raise
        except JSONDecodeError:
            data = None
        except requests.exceptions.RequestException as e:
            print(f"Errore: {e}")
            # Registra informazioni più dettagliate sull'errore se necessario
            data = None

    if data and 'results' in data and 'bindings' in data['results'] and data['results']['bindings']:
        genre_url = data['results']['bindings'][0]['genre']['value'] if 'genre' in data['results']['bindings'][0] else None
        birth_date = data['results']['bindings'][0]['birthDate']['value'] if 'birthDate' in data['results']['bindings'][0] else None
        birth_place = data['results']['bindings'][0]['birthPlaceLabel']['value'] if 'birthPlaceLabel' in data['results']['bindings'][0] else None
        genre_id = genre_url.split('/')[-1] if genre_url else None
        genre = genre_mapping.get(genre_id, None)

        return (genre, birth_date, birth_place)
    else:
        return (None, None, None)

# Registra la funzione come UDF
get_demographic_data_udf = udf(get_demographic_data, StructType([
    StructField("artist_genre", StringType(), True),
    StructField("artist_birth_date", StringType(), True),
    StructField("artist_birth_place", StringType(), True)
]))

# Carica il DataFrame da un file CSV
ratings_df = spark.read.csv("/mnt/hgfs/Condivisione/sample.csv", header=True)

# Ripartiziona il DataFrame in quattro partizioni
ratings_df = ratings_df.repartition(4)

# Aggiungi colonne per i dati demografici utilizzando l'UDF
ratings_df = ratings_df.withColumn("demographic_data", get_demographic_data_udf(ratings_df["artist_name"]))

# Estrai colonne dalla struttura demographic_data
ratings_df = ratings_df \
    .withColumn("artist_genre", ratings_df["demographic_data.artist_genre"]) \
    .withColumn("artist_birth_date", ratings_df["demographic_data.artist_birth_date"]) \
    .withColumn("artist_birth_place", ratings_df["demographic_data.artist_birth_place"])

# Elimina la colonna demographic_data
ratings_df = ratings_df.drop("demographic_data")

# Salva il DataFrame arricchito
ratings_df.write.csv("/mnt/hgfs/Condivisione/out", header=True, mode="overwrite")

# Arresta la sessione Spark
spark.stop()
