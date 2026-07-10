# Cloud Deployment Plan

This document outlines the architectural roadmap and migration strategy for moving the DataOne platform from a local `docker-compose` environment to a fully managed, production-grade cloud environment. We evaluate two major cloud providers: **Amazon Web Services (AWS)** and **Microsoft Azure**.

---

## 1. Cloud Architecture Mapping

The goal of this migration is to replace self-hosted Docker containers with Managed Platform-as-a-Service (PaaS) or Software-as-a-Service (SaaS) offerings to reduce operational overhead, ensure high availability, and scale dynamically.

| Component | Current (Docker) | AWS Target | Azure Target |
| :--- | :--- | :--- | :--- |
| **Source Database** | PostgreSQL Container | Amazon RDS for PostgreSQL | Azure Database for PostgreSQL |
| **Change Data Capture** | Kafka + Debezium | Amazon MSK (Managed Kafka) + MSK Connect | Azure Event Hubs (Kafka API) or Confluent Cloud |
| **Batch Ingestion** | Apache NiFi | EC2 / Amazon EKS (Containerized NiFi) | Azure VMs / AKS (Containerized NiFi) |
| **Data Lake Storage** | Local Disk Volume | Amazon S3 | Azure Data Lake Storage (ADLS) Gen2 |
| **Lakehouse Catalog** | Hadoop/Filesystem | AWS Glue Data Catalog | Azure Purview / Hive Metastore |
| **Data Processing (Spark)** | Spark Master/Worker | Amazon EMR or AWS Glue Ray/Spark | Azure Databricks or Synapse Spark Pools |
| **Serving Layer** | ClickHouse Server | ClickHouse Cloud (AWS Region) | ClickHouse Cloud (Azure Region) |
| **Dashboards** | Grafana | Amazon Managed Grafana | Azure Managed Grafana |
| **Orchestration** | Makefiles / Manual | Prefect Cloud / MWAA (Airflow) | Prefect Cloud / Azure Data Factory |

---

## 2. Target Architecture: AWS

If deployed on AWS, the architecture leverages native integrations for a seamless Lakehouse experience:

1. **Ingestion & Streaming:**
   - Source PostgreSQL runs on **RDS Multi-AZ** for high availability.
   - CDC events are captured using Debezium running on **MSK Connect**, pushing directly to **Amazon MSK** topics.
   - **Apache NiFi** is deployed on an **EKS (Elastic Kubernetes Service)** cluster for resilient, scalable batch extraction.
2. **Lakehouse Storage & Compute:**
   - Raw data (Bronze), Silver, and Gold layers are stored as Apache Iceberg tables backed by **Amazon S3**.
   - **AWS Glue Data Catalog** serves as the central Iceberg catalog.
   - The PySpark ETL scripts (`bronze_to_silver.py`, `scd2_customer_dim.py`) run natively as **AWS Glue Jobs** or transient **EMR Clusters**, utilizing spot instances for cost reduction during nightly batch runs.
3. **Serving & Visualization:**
   - **ClickHouse Cloud** is provisioned within the same AWS region (VPC Peering) to ingest Gold marts from S3.
   - **Amazon Managed Grafana** securely connects to ClickHouse to serve dashboards.

---

## 3. Target Architecture: Azure

If deployed on Azure, the architecture leans heavily on Databricks for Lakehouse compute:

1. **Ingestion & Streaming:**
   - Source database runs on **Azure Database for PostgreSQL - Flexible Server**.
   - Kafka CDC streams are hosted on **Azure Event Hubs** (using its Kafka compatibility layer) or Confluent on Azure.
2. **Lakehouse Storage & Compute:**
   - Iceberg data files are stored securely in **ADLS Gen2**.
   - **Azure Databricks** acts as the core processing engine. The existing PySpark jobs can be submitted as Databricks Workflows. Databricks' Unity Catalog or an external Hive metastore handles the Iceberg metadata.
3. **Serving & Visualization:**
   - **ClickHouse Cloud** runs in the Azure region.
   - **Azure Managed Grafana** connects to ClickHouse with Azure Entra ID (Active Directory) SSO integration.

---

## 4. Migration Strategy & Phases

We will adopt a **Phased Migration Approach** to minimize downtime and risk:

### Phase 1: Infrastructure as Code (IaC)
- Select the target cloud provider.
- Write **Terraform** modules to provision the base networking (VPC/VNet, Subnets, Security Groups) and storage buckets (S3/ADLS).
- Provision the managed database (RDS/Azure DB) and migrate the initial seed data.

### Phase 2: Compute & Streaming Setup
- Provision the Managed Kafka cluster (MSK/Event Hubs) and establish the Debezium CDC connectors.
- Deploy Apache NiFi onto a managed Kubernetes cluster (EKS/AKS).
- Update connection strings in `src/dataone/config.py` to use AWS Secrets Manager or Azure Key Vault for credentials.

### Phase 3: Spark & Lakehouse Integration
- Refactor the Spark Session builder (`spark_session.py`) to connect to the AWS Glue Catalog or Azure Databricks environment instead of a local Hadoop catalog.
- Execute a dry-run of the Spark Batch job (`bronze_to_silver.py`) writing directly to S3/ADLS.

### Phase 4: Serving, Dashboards & Orchestration
- Provision ClickHouse Cloud and wire it to the cloud storage buckets for native data loading.
- Import Grafana JSON dashboard definitions into the Managed Grafana instance.
- Deploy Prefect to orchestrate the dependency graph (Trigger CDC $\rightarrow$ Trigger NiFi $\rightarrow$ Run Spark Job $\rightarrow$ Trigger ClickHouse Sync).

---

## 5. Security & Networking Considerations

- **Private Networking:** All compute components (Spark, NiFi, Kafka) will reside in Private Subnets with no direct public internet access.
- **IAM/RBAC:** Strict Principle of Least Privilege. Spark jobs will assume an IAM Role / Managed Identity with exactly `s3:PutObject` or ADLS Write permissions restricted to the specific bucket.
- **Secrets Management:** Hardcoded `.env` files will be entirely replaced by AWS Secrets Manager or Azure Key Vault integrations.
