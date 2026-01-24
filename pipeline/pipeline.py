
import sys
import pandas as pd

print("arguments", sys.argv)

# El primer argumento (índice 1) será el día
day = int(sys.argv[1])

print(f"Running pipeline for day {day}")

# Prueba de pandas
df = pd.DataFrame({"A": [1, 2], "B": [3, 4]})
print(df.head())

# Guarda el resultado como un archivo parquet
df.to_parquet(f"output_day_{day}.parquet")