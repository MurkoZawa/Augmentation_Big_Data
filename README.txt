Nel file Word è presente la relazione del progetto, con spiegazioni e analisi dei risultati.

Se si volesse riproporre nel proprio cluster, seguire i seguenti step:

1) Scaricare il dataset da https://mega.nz/file/oKtWnC5Y#u9XJ-Hz0-AaIlWrCkdFSPL6nlJphtOUjsdS_mj4j1kk e spacchettarlo all'interno della directory "augmentation_big_data" (per intenderci, deve trovarsi nella stessa directory del README, del PDF della relazione e della cartella Big-Data---KGs-project, perché i path negli script sono impostati in tal modo) per ottenere i file artists.txt, tracks.txt e tracks.inter.

2) Aprire il Notebook filtered_extraction.ipynb e runnare tutte le sezioni in ordine. Al momento dell'input, immettere il numero di righe che si desidera leggere dal .inter (ex. 1 se si vuole un milione, 0.5 se si vogliono 500.000 ecc.)

3) Una volta finito, se si vuole runnare su Spark, posizionare il contenuto di "Condivisione" all'interno di un file system distribuito a cui il cluster ha accesso. IMPORTANTE: IL PERCORSO AL FILE SYSTEM DISTRIBUITO DEV'ESSERE MODIFICATO ANCHE NELLA DEFINIZIONE DEI PATH
DEI FILE IN INPUT/OUTPUT NEGLI SCRIPT augmentation_spark.py, concatenate_partitions.py e graphs_generator_fs.py

4) Per eseguire la data augmentation, installare la libreria requests con pip install requests.
Dopodiché, avviare master e worker di Spark, e utilizzare il seguente comando: 
./spark/bin/spark-submit  --master spark://<ip-master>:<porta-master>  --conf spark.executor.memory=2G  --conf spark.executor.cores=2  --conf spark.driver.memory=2G  --conf spark.executor.instances=<NUM_ISTANZE>  --num-executors <NUM_WORKER> --py-files /percorso/filesystem/distribuito/augmentation_spark.py  --files /percorso/filesystem/distribuito/sample.csv /percorso/filesystem/distribuito/augmentation_spark.py && python3 /percorso/filesystem/distribuito/concatenate_partitions.py

	3-4bis) Se si vuole in alternativa eseguire l'augmentation in un singolo nodo, aprire il Notebook augmentation_single_node.ipynb e 	runnare tutte le sezioni in ordine.

5) Per generare i grafici, installare la libreria Matplotlib con pip install matplotlib. Dopodiché, eseguire il comando python3 /percorso/filesystem/distribuito/graphs_generator_fs.py.

	5bis) In alternativa, spostare l'output ottenuto dopo lo step 4 dentro la directory in cui è presente lo script 	graphs_generator.ipynb; aprirlo e runnare tutte le sezioni in ordine.

