{
	"jobConfig": {
		"name": "ports_ref_silver",
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
		"scriptName": "ports_ref_silver.py",
		"scriptLocation": "s3://aws-glue-assets-755573863431-eu-north-1/scripts/",
		"language": "python-3",
		"spark": true,
		"sparkConfiguration": "standard",
		"jobParameters": [
			{
				"key": "--activity_path",
				"value": "s3://g9-ais-project/silver/ports_activity/ports/",
				"existing": false
			},
			{
				"key": "--conf",
				"value": "spark.eventLog.rolling.enabled=true --conf spark.sql.catalog.glue_catalog.glue.skip-name-validation=true",
				"existing": false
			},
			{
				"key": "--source_path",
				"value": "s3://g9-ais-project/bronze/ports_reference/",
				"existing": false
			},
			{
				"key": "--target_path",
				"value": "s3://g9-ais-project/silver/ports_reference/",
				"existing": false
			}
		],
		"tags": [],
		"jobMode": "DEVELOPER_MODE",
		"createdOn": "2026-10-28T03:43:56.821Z",
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
	"script": "import sys, json\nfrom awsglue.utils import getResolvedOptions\nfrom awsglue.context import GlueContext\nfrom awsglue.job import Job\nfrom pyspark.context import SparkContext\nfrom pyspark.sql import functions as F\n\nargs = getResolvedOptions(sys.argv, [\"JOB_NAME\", \"source_path\", \"target_path\", \"activity_path\"])\n\nsc = SparkContext()\nglue = GlueContext(sc)\nspark = glue.spark_session\njob = Job(glue)\njob.init(args[\"JOB_NAME\"], args)\n\nref = spark.read.parquet(args[\"source_path\"])\n\ndef blank_to_null(c):\n    return F.when(F.trim(F.col(c)) == \"\", None).otherwise(F.trim(F.col(c)))\n\nsilver = ref.select(\n    \"portid\",\n    F.trim(\"portname\").alias(\"portname\"),\n    F.trim(\"country\").alias(\"country\"),\n    F.col(\"countrynoaccents\").alias(\"country_ascii\"),\n    F.upper(F.trim(\"ISO3\")).alias(\"iso3\"),\n    F.trim(\"continent\").alias(\"continent\"),\n    F.trim(\"fullname\").alias(\"fullname\"),\n    F.col(\"lat\").cast(\"double\").alias(\"lat\"),\n    F.col(\"lon\").cast(\"double\").alias(\"lon\"),\n    F.col(\"vessel_count_total\").cast(\"int\").alias(\"vessel_count_total\"),\n    F.col(\"vessel_count_container\").cast(\"int\").alias(\"vessel_count_container\"),\n    F.col(\"vessel_count_dry_bulk\").cast(\"int\").alias(\"vessel_count_dry_bulk\"),\n    F.col(\"vessel_count_general_cargo\").cast(\"int\").alias(\"vessel_count_general_cargo\"),\n    F.col(\"vessel_count_RoRo\").cast(\"int\").alias(\"vessel_count_roro\"),\n    F.col(\"vessel_count_tanker\").cast(\"int\").alias(\"vessel_count_tanker\"),\n    blank_to_null(\"industry_top1\").alias(\"industry_top1\"),\n    blank_to_null(\"industry_top2\").alias(\"industry_top2\"),\n    blank_to_null(\"industry_top3\").alias(\"industry_top3\"),\n    F.col(\"share_country_maritime_import\").cast(\"double\").alias(\"share_country_maritime_import\"),\n    F.col(\"share_country_maritime_export\").cast(\"double\").alias(\"share_country_maritime_export\"),\n    blank_to_null(\"LOCODE\").alias(\"locode\"),\n).cache()\n\n# ---- quality checks ----\nactivity_ports = spark.read.parquet(args[\"activity_path\"]).select(\"portid\").distinct()\nreport = silver.agg(\n    F.count(\"*\").alias(\"rows\"),\n    F.countDistinct(\"portid\").alias(\"distinct_portid\"),\n    F.sum(F.col(\"portid\").isNull().cast(\"int\")).alias(\"null_portid\"),\n    F.sum((F.col(\"lat\").isNull() | F.col(\"lon\").isNull()).cast(\"int\")).alias(\"null_coords\"),\n    F.sum(((F.col(\"lat\") < -90) | (F.col(\"lat\") > 90) | (F.col(\"lon\") < -180) | (F.col(\"lon\") > 180)).cast(\"int\")).alias(\"bad_coords\"),\n    F.sum((F.col(\"vessel_count_total\") < 0).cast(\"int\")).alias(\"negative_counts\"),\n    F.sum(F.col(\"locode\").isNull().cast(\"int\")).alias(\"null_locode\"),\n).collect()[0].asDict()\nreport[\"activity_ports_missing_in_ref\"] = activity_ports.join(silver.select(\"portid\"), \"portid\", \"left_anti\").count()\nreport[\"ref_ports_without_activity\"] = silver.select(\"portid\").join(activity_ports, \"portid\", \"left_anti\").count()\n\nprint(\"QUALITY REPORT: \" + json.dumps(report, default=str))\n\nhard = {k: v for k, v in report.items()\n        if k in (\"null_portid\", \"null_coords\", \"bad_coords\", \"negative_counts\", \"activity_ports_missing_in_ref\") and v != 0}\nif report[\"rows\"] != report[\"distinct_portid\"]:\n    hard[\"portid_not_unique\"] = report[\"rows\"] - report[\"distinct_portid\"]\nassert not hard, f\"Quality checks failed: {hard}\"\n\n# small dimension: full overwrite, one file\nsilver.coalesce(1).write.mode(\"overwrite\").parquet(args[\"target_path\"])\n\njob.commit()"
}