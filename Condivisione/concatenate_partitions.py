import pandas as pd
import glob
import os
import csv


# Verifica se il file di input esiste
pattern = "part-0000*.csv"

# Lista dei percorsi dei file CSV da concatenare
csv_paths = glob.glob("/mnt/hgfs/Condivisione/out/" + pattern)

# Leggi i file CSV in una lista di DataFrame pandas
dfs = [pd.read_csv(path, usecols=range(9)) for path in csv_paths]

# Aggiungi una colonna "source" ai singoli DataFrame per identificare la provenienza dei dati
for i, df in enumerate(dfs):
    df["source"] = f"file{i + 1}"

# Concatena i DataFrame in uno unico
concatenated_df = pd.concat(dfs, ignore_index=True)

# Stampa il contenuto del DataFrame concatenato
print("Contenuto del DataFrame concatenato:")
print(concatenated_df)

# Salva il DataFrame concatenato in un nuovo file CSV
concatenated_df.to_csv("/mnt/hgfs/Condivisione/out/user_augmentation.csv", index=False)

# Elimina i file CSV originali
for path in csv_paths:
    os.remove(path)

print("File CSV originali eliminati.")
