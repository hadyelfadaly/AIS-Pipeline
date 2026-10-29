{
	"jobConfig": {
		"name": "ais_delta_maintenance",
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
		"scriptName": "ais_delta_maintenance.py",
		"scriptLocation": "s3://aws-glue-assets-755573863431-eu-north-1/scripts/",
		"language": "python-3",
		"spark": false,
		"sparkConfiguration": "standard",
		"jobParameters": [
			{
				"key": "--base_path",
				"value": "s3://g9-ais-project/silver",
				"existing": false
			},
			{
				"key": "--conf",
				"value": "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog --conf spark.eventLog.rolling.enabled=true --conf spark.sql.catalog.glue_catalog.glue.skip-name-validation=true",
				"existing": false
			},
			{
				"key": "--datalake-formats",
				"value": "delta",
				"existing": false
			},
			{
				"key": "--retention_hours",
				"value": "168",
				"existing": false
			},
			{
				"key": "--tables",
				"value": "positions,vessels",
				"existing": false
			}
		],
		"tags": [],
		"jobMode": "DEVELOPER_MODE",
		"createdOn": "2026-10-29T00:42:59.497Z",
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
	"script": "import sys\nfrom awsglue.utils import getResolvedOptions\nfrom awsglue.context import GlueContext\nfrom pyspark.context import SparkContext\n\nargs = getResolvedOptions(sys.argv, [\"base_path\", \"tables\", \"retention_hours\"])\n\nspark = GlueContext(SparkContext.getOrCreate()).spark_session\n\nbase = args[\"base_path\"].rstrip(\"/\")\nretention = int(args[\"retention_hours\"])\nif retention < 168:\n    raise ValueError(\"retention_hours below 168 (7 days) risks breaking readers and the stream\")\n\nfailures = []\nfor table in args[\"tables\"].split(\",\"):\n    path = f\"{base}/{table.strip()}\"\n    try:\n        spark.sql(f\"DESCRIBE DETAIL delta.`{path}`\").select(\"numFiles\", \"sizeInBytes\").show()\n        print(f\"OPTIMIZE {path}\", flush=True)\n        spark.sql(f\"OPTIMIZE delta.`{path}`\").show(truncate=False)\n        print(f\"VACUUM {path}\", flush=True)\n        spark.sql(f\"VACUUM delta.`{path}` RETAIN {retention} HOURS\").show(truncate=False)\n    except Exception as e:\n        print(f\"FAILED {path}: {e}\", flush=True)\n        failures.append(table)   # one table failing shouldn't skip the other\n\nif failures:\n    raise RuntimeError(f\"Delta maintenance failed for: {failures}\")"
}