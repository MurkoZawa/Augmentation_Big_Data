import pandas as pd
import glob
import os
import csv

# File di input e output
input_file = "/mnt/hgfs/Condivisione/user_augmentation/user_augmentation.csv"
output_file = "user_augmentation_fixed.csv"
delimiter = ','  # Adatta il delimitatore in base al tuo file

# Verifica se il file di input esiste
pattern = "part-0000*.csv"
if os.path.exists(input_file):
    # Apri il file di input in modalità lettura e il file di output in modalità scrittura
    with open(input_file, 'r', encoding='utf-8') as infile, open(output_file, 'w', encoding='utf-8', newline='') as outfile:
        reader = csv.reader(infile, delimiter=delimiter)
        writer = csv.writer(outfile, delimiter=delimiter)

        # Copia l'header nel nuovo file
        header = next(reader)
        writer.writerow(header[:9])  # Riduci l'header a 9 colonne

        # Copia il contenuto del file, riducendo ogni riga a 9 colonne
        for line_number, line in enumerate(reader, start=2):
            reduced_line = line[:9]  # Riduci la riga a 9 colonne
            writer.writerow(reduced_line)

    print(f"File {output_file} creato con successo.")
    os.remove(input_file)
else:
    print(f"Il file {input_file} non esiste.")

# Lista dei percorsi dei file CSV da concatenare
csv_paths = glob.glob("/mnt/hgfs/Condivisione/user_augmentation/" + pattern)

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
concatenated_df.to_csv("/mnt/hgfs/Condivisione/user_augmentation/user_augmentation.csv", index=False)

# Elimina i file CSV originali
for path in csv_paths:
    os.remove(path)

print("File CSV originali eliminati.")
