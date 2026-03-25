# 🌍 Climate Data Engineering Pipeline (GCP + Terraform + Bruin)

An end-to-end **batch data engineering pipeline** built on Google Cloud Platform (GCP). The pipeline ingests public climate datasets, stores raw files in a cloud data lake (GCS), transforms them in a data warehouse (BigQuery), and visualises the results in a Looker Studio dashboard.

Built as a portfolio project demonstrating modern data engineering practices:
- Infrastructure as Code (Terraform)
- Workflow orchestration (Bruin)
- Cloud data lake (Google Cloud Storage)
- Analytical data warehouse with partitioning and clustering (BigQuery)
- Data visualisation (Looker Studio)

---

# 🏗️ Architecture

The pipeline follows a structured flow from raw data ingestion to analytics:

1. **Extract**  
   Python scripts fetch climate data from **Our World in Data**.

2. **Load (Data Lake)**  
   Raw CSV files are stored in **Google Cloud Storage (GCS)**.

3. **Stage (Data Warehouse)**  
   Data is loaded into **BigQuery staging tables**.

4. **Transform (Data Warehouse Mart)**  
   SQL transformations aggregate and model the data into optimized **BigQuery fact tables** (partitioned by year, clustered by entity).

5. **Visualize**  
   Insights are presented through a **Looker Studio dashboard**.

   **Why two layers in BigQuery (staging + mart)?**
   Staging is a direct copy of what is in GCS — no transformation, no business logic. If something goes wrong in a transformation, you can always re-run the SQL against staging without re-downloading the source data. The mart is what dashboards connect to — it is clean, optimised, and never changes shape unexpectedly.
---

# 🛠️ Technology Stack

| Layer | Technology | Why |
|---|---|---|
| Infrastructure as Code | Terraform 1.7 | Reproducible, version-controlled GCP resources |
| Cloud Platform | GCP | BigQuery + GCS are best-in-class for analytical workloads |
| Data Lake | Google Cloud Storage | Cheap, durable object storage for raw CSV files |
| Orchestration | Bruin CLI | Lightweight pipeline orchestration with built-in GCP connectors |
| Data Warehouse | BigQuery | Serverless, scalable SQL warehouse with partitioning/clustering |
| Visualisation | Looker Studio | Free, native BigQuery connector, shareable dashboards |
| Language | Python 3 | pandas + google-cloud-* libraries |
| Dependency Management | uv | Fast, reliable Python package management |

---

# 📊 Datasets Used

The pipeline uses publicly available climate datasets from **Our World in Data**:

### 🌡️ Global Temperature Anomalies
Yearly deviations from long-term global temperature averages.

### 🌊 Global Sea Level Rise
Historical measurements tracking changes in global mean sea level.

---

# 📂 Project Structure

```text
climate-correlation-engine/
├── .devcontainer/
│   ├── devcontainer.json       # Codespaces container definition
│   └── setup.sh                # Auto-installs gcloud, Bruin CLI, uv, Python deps
│
├── terraform/
│   ├── provider.tf             # Google provider version pin
│   ├── variables.tf            # project_id, region, bq_location
│   ├── main.tf                 # GCS bucket + BigQuery datasets + tables
│   ├── outputs.tf              # Prints resource names after apply
│   └── terraform.tfvars.example  # Template — copy to terraform.tfvars
│
├── pipelines/
│   ├── temperature_pipeline/
│   │       ├── pipeline.yml        # Bruin schedule + venv config
│   │       └── assets/ 
│   │            ├── bigquery/
│   │            │     └── temperature_model.sql   # staging → mart (5yr rolling avg)
│   │            ├── ingestion/
│   │            │     └── download_temperature.py # Download temperature CSV → upload to GCS
│   │            └── staging/
│   │                   └── load_to_staging.py     # GCS → BigQuery staging
│   │
│   └── sea_level_pipeline/
│           ├── pipeline.yml        # Bruin schedule + venv config
│           └── assets/ 
│                ├── bigquery/
│                │     └── sea_level_model.sql     # staging → mart (YoY change in mm)
│                ├── ingestion/
│                │     └── download_sea_level.py   # Download sea level CSV → upload to GCS
│                └── staging/
│                      └── load_to_staging.py     # GCS → BigQuery staging                 
│
├── scripts/
│   └── run_pipeline.sh         # Runs all steps end-to-end
│
├── bruin.config.yml.example    # Bruin credentials template
├── requirements.txt            # Python dependencies
├── .gitignore
└── README.md
```
---

## 🚀 Setup & Execution

### Step 1 — Open in GitHub Codespaces

1. Push this repository to GitHub.
2. Click **Code → Codespaces → Create codespace on main**.
3. Codespaces will automatically run `.devcontainer/setup.sh` which installs:
   - Google Cloud SDK (`gcloud`, `bq`, `gsutil`)
   - Bruin CLI
   - Python uv + all packages from `requirements.txt` into an isolated venv at `.venv/`
4. Wait for the green "Devcontainer setup complete" message in the terminal.

**What the venv is for:**
The `.venv` is a Python virtual environment managed by `uv`. All your ingestion and pipeline scripts run inside it. Bruin is configured to use `.venv/bin/python` as its interpreter, so it always uses the same packages you installed — no version conflicts, no "works on my machine" problems.

---

### Step 2 — Create a GCP Project

1. Go to [console.cloud.google.com](https://console.cloud.google.com)
2. Create a new project (e.g. `climate-pipeline-123`). Note the **Project ID** — it looks like `climate-pipeline-123`, not the display name.
3. Enable billing on the project (required for BigQuery and GCS).
4. Enable these APIs:
   ```
   BigQuery API
   Cloud Storage API
   ```
   You can enable them via the Console (search "API Library") or with:
   ```bash
   gcloud services enable bigquery.googleapis.com storage.googleapis.com \
     --project=your-project-id
   ```

---

### Step 3 — Authenticate with GCP

Inside Codespaces terminal:

```bash
# Log in with your Google account — opens a browser link
gcloud auth application-default login

# Set your active project
gcloud config set project your-project-id
```

**What application-default login does:**
It creates a credentials file at `~/.config/gcloud/application_default_credentials.json`. The Python `google-cloud-*` libraries automatically pick this file up — you don't need to pass credentials anywhere in code. This is the standard approach for development environments.

---

### Step 4 — Provision GCP Infrastructure with Terraform

Terraform creates the GCS bucket and BigQuery datasets/tables. You only need to run this once (or again if you destroy and recreate).

```bash
# Copy the example vars file and fill in your project ID
cp terraform/terraform.tfvars.example terraform/terraform.tfvars
# Edit terraform.tfvars and set: project_id = "your-project-id"

cd terraform
terraform init      # downloads the Google provider plugin
terraform plan      # shows what will be created — review before applying
terraform apply     # type 'yes' to confirm and create resources
cd ..
```

**What Terraform creates:**

| Resource | Name | Purpose |
|---|---|---|
| GCS Bucket | `your-project-id-climate-data-lake` | Raw CSV files (data lake) |
| BQ Dataset | `climate_staging` | Direct copy of raw GCS data, no transformation |
| BQ Table | `stg_temperature` | Raw temperature rows |
| BQ Table | `stg_sea_level` | Raw sea level rows |
| BQ Dataset | `climate_mart` | Analytics-ready, dashboard-facing data |
| BQ Table | `fact_temperature` | Partitioned by year, clustered by entity |
| BQ Table | `fact_sea_level` | Partitioned by year, clustered by entity |

**Why partitioning and clustering matter:**
BigQuery charges per byte scanned. A dashboard query like "show global temperature from 1990 to 2020" on an unpartitioned table scans every row since 1880. With year-range partitioning, BigQuery skips all partitions outside 1990–2020 automatically — typically an 80–90% cost reduction on this dataset. Clustering by `entity` means filtering to `WHERE entity = 'World'` reads only that entity's sorted blocks, not the full partition.

---

### Step 5 — Configure Bruin Credentials

Bruin needs a service account to connect to BigQuery on your behalf. Or for development environment testing you can ommit the service_account_file path and let Bruin use the Application Default Credentials (ADC) to Authenticate when running the project.

**Create a service account:**
```bash
# Create the service account
gcloud iam service-accounts create bruin-runner \
  --display-name="Bruin Pipeline Runner" \
  --project=your-project-id

# Grant it the required roles
gcloud projects add-iam-policy-binding your-project-id \
  --member="serviceAccount:bruin-runner@your-project-id.iam.gserviceaccount.com" \
  --role="roles/bigquery.dataEditor"

gcloud projects add-iam-policy-binding your-project-id \
  --member="serviceAccount:bruin-runner@your-project-id.iam.gserviceaccount.com" \
  --role="roles/bigquery.jobUser"

gcloud projects add-iam-policy-binding your-project-id \
  --member="serviceAccount:bruin-runner@your-project-id.iam.gserviceaccount.com" \
  --role="roles/storage.objectAdmin"

# Download the JSON key
mkdir -p ~/.gcp
gcloud iam service-accounts keys create ~/.gcp/bruin-service-account.json \
  --iam-account=bruin-runner@your-project-id.iam.gserviceaccount.com
```

**Create the Bruin config:**
```bash
mkdir -p ~/.bruin
cp bruin.config.yml.example ~/.bruin.yml
# Edit ~/.bruin.yml and set your project_id
```

---

### Step 6 — Set Environment Variables

The pipeline scripts read `GCS_BUCKET` and `GCP_PROJECT` from environment variables. Add them to your shell session (or append to `~/.bashrc` to persist):

```bash
export GCS_BUCKET="your-project-id-climate-data-lake"
export GCP_PROJECT="your-project-id"
```

To persist across Codespaces restarts:
```bash
echo 'export GCS_BUCKET="your-project-id-climate-data-lake"' >> ~/.bashrc
echo 'export GCP_PROJECT="your-project-id"' >> ~/.bashrc
```

---

### Step 7 — Run the Pipeline

```bash
bash scripts/run_pipeline.sh
```

This runs three steps in order:

**Step 1 — Ingest:** Downloads CSVs from Our World in Data and uploads them to your GCS bucket under `raw/temperature/` and `raw/sea_level/`.

**Step 2 — Stage:** Reads the CSVs from GCS and loads them into `climate_staging.stg_temperature` and `climate_staging.stg_sea_level` in BigQuery. Each run fully replaces the staging tables (WRITE_TRUNCATE) — staging always reflects the latest raw data.

**Step 3 — Transform:** Runs the SQL in `bigquery/` against BigQuery. This creates `climate_mart.fact_temperature` and `climate_mart.fact_sea_level` with:
- 5-year rolling average for temperature (smooths year-to-year noise)
- Year-over-year sea level change in mm

You can also run Bruin's orchestrated pipeline directly:
```bash
bruin run pipelines/temperature_pipeline/pipeline.yml
bruin run pipelines/sea_level_pipeline/pipeline.yml
```

---

## 📈 Analytics Dashboard

The [dashboard](https://lookerstudio.google.com/s/prJqqQKm6lI) provides three primary analytical views:

* **Global Temperature Trend**: A line chart showing annual temperature anomalies.
- Dimension: `year`
- Metric: `anomaly_5yr_avg`
- Filter: `entity = 'World'` `start_end_year > '1992'` `start_end_year < '2021'`
- This shows the long-term warming trend as a smooth line

* **Hemsiphere Temperature Distribution**: A bar chart showing the hemsiphere annual temperature anomalies.
- Dimension: `year`
- Metric: `temperature_anomaly`
- Filter: `hemsiphere = 'Southern Hemisphere'` `hemsiphere = 'Northern Hemisphere'` `start_end_year > '1992'` `start_end_year < '2021'`
- This shows stacked bar chart of the hemsipheres warming.

* **Sea Level Change**: A line chart visualizing long-term rising sea levels.
- Dimension: `year`
- Metric: `sea_level_change`
- Filter: `entity = 'World'` `start_end_year > '1992'` `start_end_year < '2021'`
- This shows cumulative sea level rise in mm.

---

## 🔄 Daily Automated Runs (Bruin Scheduling)

The `pipeline.yml` files set `schedule: "0 2 * * *"` — daily at 02:00 UTC. To activate Bruin's scheduler daemon:

```bash
bruin schedule start
```

Bruin will run both pipelines daily, re-downloading the latest data, refreshing staging, and updating the mart. Since Our World in Data updates annually, the daily run is primarily for resilience — it ensures the pipeline stays healthy and your infrastructure stays warm.

---

## 📈 Data Model

### `climate_mart.fact_temperature`

| Column | Type | Description |
|---|---|---|
| year | INTEGER | Calendar year |
| entity | STRING | Country or region (e.g. 'World') |
| temperature_anomaly | FLOAT | Annual deviation from 1951–1980 baseline (°C) |
| anomaly_5yr_avg | FLOAT | 5-year rolling average anomaly (°C) |

Partitioned by: `year` (range 1850–2100, interval 10)
Clustered by: `entity`

### `climate_mart.fact_sea_level`

| Column | Type | Description |
|---|---|---|
| year | INTEGER | Calendar year |
| entity | STRING | Country or region |
| sea_level_change | FLOAT | Cumulative rise from 1880 baseline (mm) |
| yoy_change_mm | FLOAT | Year-over-year change (mm) |

Partitioned by: `year` (range 1850–2100, interval 10)
Clustered by: `entity`

---

## 🔮 Future Improvements

- **Data quality checks** — validate row counts, null rates, and anomaly bounds before loading to mart
- **Incremental loading** — only process new years rather than full refresh each run
- **CI/CD** — GitHub Actions to validate Terraform plans and SQL on pull requests
- **Schema detection** — auto-detect column renames in Our World in Data source files
- **Additional datasets** — CO₂ emissions, Arctic ice extent, extreme weather events

---

## 🔒 Security Notes

- `terraform.tfvars` is gitignored — never commit your project ID combined with any credentials
- Service account keys (`~/.gcp/`) are stored outside the repo in your home directory
- `~/.bruin.yml` is stored outside the repo — never commit it
- Application default credentials (`~/.config/gcloud/`) are Codespaces-local and not persisted to the repo

---

**Author:** Data Engineering Portfolio Project
