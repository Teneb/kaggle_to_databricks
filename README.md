# kaggle_to_databricks
ważne punkty:

notebook.py - notebook do importu w databricks
w folderze KaggleToDatabricks są pliki które wysłałem do PyPi.org

zanim odpalimy joba/notebook potrzebujemy stworzyć ścieżki poniższymi komendami

dbutils.fs.mkdirs("dbfs:/FileStore/Kaggle_Token/")

dbutils.fs.mkdirs("dbfs:/FileStore/Kaggle_Datasets/")

tworząc joba z notebooka pamiętaj że trzeba podać ścieżki jako parametry danych nie ścieżki z nazwą pliku! (i stworzyć je korzystając z komendy dbutils.fs.mkdirs)

unit test znajduje się w paczce, oraz w notebooku
