{
	"jobConfig": {
		"name": "ports_silver_weekly",
		"description": "",
		"role": "arn:aws:iam::755573863431:role/g9-glue-silver-role",
		"command": "glueetl",
		"version": "5.0",
		"currentGlueVersion": "",
		"runtime": null,
		"workerType": "G.1X",
		"numberOfWorkers": 2,
		"maxCapacity": 2,
		"jobRunQueuingEnabled": false,
		"maxRetries": 0,
		"timeout": 10,
		"maxConcurrentRuns": 1,
		"security": "none",
		"scriptName": "ports_silver_weekly.py",
		"scriptLocation": "s3://aws-glue-assets-755573863431-eu-north-1/scripts/",
		"language": "python-3",
		"spark": true,
		"sparkConfiguration": "standard",
		"jobParameters": [
			{
				"key": "--conf",
				"value": "spark.eventLog.rolling.enabled=true --conf spark.sql.catalog.glue_catalog.glue.skip-name-validation=true",
				"existing": false
			},
			{
				"key": "--lookback_days",
				"value": "14",
				"existing": false
			},
			{
				"key": "--source_path",
				"value": "s3://g9-ais-project/bronze/ports_activity/ports/",
				"existing": false
			},
			{
				"key": "--target_path",
				"value": "s3://g9-ais-project/silver/ports_activity/ports/",
				"existing": false
			}
		],
		"tags": [],
		"jobMode": "DEVELOPER_MODE",
		"createdOn": "2026-10-28T03:05:11.574Z",
		"developerMode": true,
		"connectionsList": [],
		"temporaryDirectory": "s3://aws-glue-assets-755573863431-eu-north-1/temporary/",
		"glueHiveMetastore": true,
		"etlAutoTuning": true,
		"metrics": true,
		"observabilityMetrics": true,
		"bookmark": "job-bookmark-disable",
		"sparkPath": "s3://aws-glue-assets-755573863431-eu-north-1/sparkHistoryLogs/",
		"maintenanceWindow": null
	},
	"hasBeenSaved": false,
	"usageProfileName": null,
	"script": "import sys, json\nfrom datetime import date, datetime, timedelta\nfrom awsglue.utils import getResolvedOptions\nfrom awsglue.context import GlueContext\nfrom awsglue.job import Job\nfrom pyspark.context import SparkContext\nfrom pyspark.sql import functions as F\nfrom pyspark.sql.window import Window\n\n# ---- args (lookback_days / start_date are optional) ----\nrequired = [\"JOB_NAME\", \"source_path\", \"target_path\"]\nfor opt in [\"lookback_days\", \"start_date\"]:\n    if f\"--{opt}\" in sys.argv:\n        required.append(opt)\nargs = getResolvedOptions(sys.argv, required)\n\nlookback = int(args.get(\"lookback_days\", 28))\nstart_arg = args.get(\"start_date\", \"\")\n\nsc = SparkContext()\nglue = GlueContext(sc)\nspark = glue.spark_session\nspark.conf.set(\"spark.sql.sources.partitionOverwriteMode\", \"dynamic\")\njob = Job(glue)\njob.init(args[\"JOB_NAME\"], args)\n\n# ---- window: snap to the first of the month so rewritten partitions are complete ----\ntoday = datetime.utcnow().date()\nstart = date.fromisoformat(start_arg) if start_arg else today - timedelta(days=lookback)\nstart = start.replace(day=1)\nassert start >= date(2026, 8, 1), (\n    f\"window start {start} is inside the CSV/API overlap period; use the backfill job for that\"\n)\nprint(f\"WINDOW: {start} -> {today}\")\n\n# ---- read only the bronze year partitions we need ----\nsrc = args[\"source_path\"].rstrip(\"/\") + \"/\"\npaths = [f\"{src}year={y}/\" for y in range(start.year, today.year + 1)]\ndf = spark.read.option(\"basePath\", src).parquet(*paths)\n\nnum_cols = [c for c in df.columns if c.startswith((\"portcalls\", \"import\", \"export\"))]\nrename = {\"portcalls\": \"portcalls_total\", \"import\": \"import_total\", \"export\": \"export_total\"}\n\n# API rows only (dash dates); the CSV backfill is already in silver\nd = (df.filter(F.col(\"date\").contains(\"-\"))\n       .withColumn(\"date_parsed\", F.to_date(\"date\", \"yyyy-MM-dd\"))\n       .filter(F.col(\"date_parsed\") >= F.lit(start.isoformat()))\n       .withColumn(\"ingest_date\",\n                   F.regexp_extract(F.input_file_name(), r\"part-weekly-(\\d{4}-\\d{2}-\\d{2})\", 1)))\n\nw = Window.partitionBy(\"portid\", \"date_parsed\").orderBy(\n    F.desc(\"ingest_date\"), F.desc(F.col(\"ObjectId\").cast(\"long\")))\n\ndedup = d.withColumn(\"rn\", F.row_number().over(w)).filter(\"rn = 1\")\n\ndef typed(c):\n    t = \"int\" if c.startswith(\"portcalls\") else \"long\"\n    return F.col(f\"`{c}`\").cast(t).alias(rename.get(c, c))\n\nsilver = dedup.select(\n    \"portid\",\n    F.col(\"date_parsed\").alias(\"date\"),\n    *[typed(c) for c in num_cols],\n    F.year(\"date_parsed\").alias(\"year\"),\n    F.month(\"date_parsed\").alias(\"month\"),\n).cache()\n\n# ---- quality checks ----\ndef quality_report(x):\n    aggs = [\n        F.count(\"*\").alias(\"rows\"),\n        F.countDistinct(\"portid\").alias(\"ports\"),\n        F.sum(F.col(\"date\").isNull().cast(\"int\")).alias(\"null_date\"),\n        F.min(\"date\").alias(\"min_date\"),\n        F.max(\"date\").alias(\"max_date\"),\n    ]\n    for p in [\"portcalls\", \"import\", \"export\"]:\n        parts = sum(F.col(f\"{p}_{k}\") for k in [\"container\", \"dry_bulk\", \"general_cargo\", \"roro\"])\n        d_cargo = F.col(f\"{p}_cargo\") - parts\n        d_total = F.col(f\"{p}_total\") - (F.col(f\"{p}_cargo\") + F.col(f\"{p}_tanker\"))\n        tol_c, tol_t = (0, 0) if p == \"portcalls\" else (3, 1)\n        aggs += [\n            F.sum(((d_cargo < 0) | (d_cargo > tol_c)).cast(\"int\")).alias(f\"{p}_cargo_out_of_tol\"),\n            F.sum(((d_total < 0) | (d_total > tol_t)).cast(\"int\")).alias(f\"{p}_total_out_of_tol\"),\n            F.sum((F.col(f\"{p}_total\") < 0).cast(\"int\")).alias(f\"{p}_negative\"),\n    ]\n    r = x.agg(*aggs).collect()[0].asDict()\n    r[\"dup_keys\"] = x.groupBy(\"portid\", \"date\").count().filter(\"count > 1\").count()\n    # days where some ports are missing compared with the fullest day\n    per_day = x.groupBy(\"date\").count()\n    full = per_day.agg(F.max(\"count\")).collect()[0][0]\n    r[\"short_days\"] = per_day.filter(F.col(\"count\") < full).count()\n    return r\n\nreport = quality_report(silver)\nprint(\"QUALITY REPORT: \" + json.dumps(report, default=str))\n\nhard = {k: v for k, v in report.items()\n        if (k.endswith((\"_negative\", \"_out_of_tol\")) or k in (\"null_date\", \"dup_keys\")) and v != 0}\nwarn = {k: v for k, v in report.items() if k == \"short_days\" and v != 0}\n\nif warn:\n    print(\"QUALITY WARNINGS: \" + json.dumps(warn))\nassert report[\"rows\"] > 0, \"no rows in the window: is the API DAG up to date?\"\nassert not hard, f\"Quality checks failed: {hard}\"\n\n# ---- write: overwrites only the year/month partitions in the window ----\n(silver.repartition(\"year\", \"month\")\n       .write.mode(\"overwrite\")\n       .partitionBy(\"year\", \"month\")\n       .parquet(args[\"target_path\"]))\n\njob.commit()"
}