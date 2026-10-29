# NiFi Flows Guide

Apache NiFi is used in DataOne to orchestrate external ingestions into the staging area before PySpark picks them up.

## 🔀 Process Groups Overview
The high-level NiFi canvas groups workflows into logical domains.
![NiFi Overview](images/nifi-flow-overview-process-groups.png)

## 📝 Campaign CSV Ingestion
This flow listens for incoming static CSV files containing marketing campaigns, standardizes them, and drops them into the lakehouse staging volume.
![CSV Ingestion](images/nifi-flow-campaign-csv-ingestion.png)

## 🌐 REST API Ingestion to MongoDB
This flow provides a plain-HTTP `ListenHTTP` endpoint for ingesting product reviews, processing the JSON payloads, and landing them into MongoDB for the subsequent batch ETL extraction.
![REST API Ingestion](images/nifi-flow-rest-api-ingestion-to-mongodb.png)

## Secrets in exported flows

`src/nifi_flows/Reviews_API_Ingestion.json` ships with a placeholder instead of
the real MongoDB password: `mongodb://dataone:<MONGO_PASSWORD>@mongodb:27017`.
After importing the flow, edit the MongoDB controller service's `mongo-uri`
property and substitute the value of `MONGO_PASSWORD` from your `.env`
(URL-encode it if it contains special characters). Never commit an export that
contains the real value.
