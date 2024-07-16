import pandas
import sys

pandas.read_csv(sys.argv[1]).to_parquet(sys.argv[2])
