import sys
import json
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window

args = getResolvedOptions(sys.argv, ["JOB_NAME", "source_path", "target_path"])

sc = SparkContext()
glue = GlueContext(sc)
spark = glue.spark_session
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
job = Job(glue)
job.init(args["JOB_NAME"], args)

df = spark.read.parquet(args["source_path"])

num_cols = [c for c in df.columns if c.startswith(("portcalls", "import", "export"))]
rename = {"portcalls": "portcalls_total", "import": "import_total", "export": "export_total"}

# parse both date formats (CSV backfill = slashes, API = dashes)
d = df.withColumn(
    "date_parsed",
    F.coalesce(F.to_date("date", "yyyy-MM-dd"), F.to_date("date", "yyyy/MM/dd")),
)

# API (dash) wins over CSV (slash) on overlap; ObjectId breaks ties
w = Window.partitionBy("portid", "date_parsed").orderBy(
    F.desc("src_rank"), F.desc(F.col("ObjectId").cast("long"))
)
dedup = (
    d.withColumn("src_rank", F.when(F.col("date").contains("-"), 1).otherwise(0))
     .withColumn("rn", F.row_number().over(w))
     .filter("rn = 1")
)

def typed(c):
    t = "int" if c.startswith("portcalls") else "long"
    return F.col(f"`{c}`").cast(t).alias(rename.get(c, c))

silver = dedup.select(
    "portid",
    F.col("date_parsed").alias("date"),
    *[typed(c) for c in num_cols],
    "year",
    F.month("date_parsed").alias("month"),
).filter(F.col("date").isNotNull())

def quality_report(df):
    aggs = [
        F.count("*").alias("rows"),
        F.countDistinct("portid").alias("ports"),
        F.sum(F.col("date").isNull().cast("int")).alias("null_date"),
        F.min("date").alias("min_date"),
        F.max("date").alias("max_date"),
    ]
    for p in ["portcalls", "import", "export"]:
        parts = sum(F.col(f"{p}_{k}") for k in ["container", "dry_bulk", "general_cargo", "roro"])
        aggs += [
            F.sum((F.col(f"{p}_cargo") != parts).cast("int")).alias(f"{p}_cargo_mismatch"),
            F.sum((F.col(f"{p}_total") != F.col(f"{p}_cargo") + F.col(f"{p}_tanker")).cast("int")).alias(f"{p}_total_mismatch"),
            F.sum((F.col(f"{p}_total") < 0).cast("int")).alias(f"{p}_negative"),
        ]
    r = df.agg(*aggs).collect()[0].asDict()
    r["dup_keys"] = df.groupBy("portid", "date").count().filter("count > 1").count()
    return r

report = quality_report(silver)
print("QUALITY REPORT: " + json.dumps(report, default=str))

hard = {k: v for k, v in report.items()
        if (k.endswith("_negative") or k in ("null_date", "dup_keys")) and v != 0}
warn = {k: v for k, v in report.items() if k.endswith("_mismatch") and v != 0}

if warn:
    print("QUALITY WARNINGS: " + json.dumps(warn))
assert not hard, f"Quality checks failed: {hard}"

(silver.repartition("year", "month")
       .write.mode("overwrite")
       .partitionBy("year", "month")
       .parquet(args["target_path"]))

job.commit()