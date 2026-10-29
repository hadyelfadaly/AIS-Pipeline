# delta_maintenance.py (simplified)
import argparse
from pyspark.sql import SparkSession

parser = argparse.ArgumentParser()
parser.add_argument('--action', required=True)
parser.add_argument('--retention-hours', default='168')
args = parser.parse_args()

spark = SparkSession.builder.appName("DeltaMaintenance").getOrCreate()

tables = ["silver.positions", "silver.vessels", "silver.weather"]

if args.action == "optimize":
    for t in tables:
        spark.sql(f"OPTIMIZE {t}")
elif args.action == "vacuum":
    for t in tables:
        spark.sql(f"VACUUM {t} RETAIN {args.retention_hours} HOURS")