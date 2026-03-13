# 🌍 Climate Data Engineering Pipeline (GCP + Terraform + Bruin)

This project demonstrates a complete **end-to-end batch data engineering pipeline** built on **Google Cloud Platform (GCP)**. It ingests public climate datasets, stores them in a cloud-based data lake, and transforms them in a data warehouse to power an analytics dashboard.

The project highlights modern data engineering practices including:

- **Infrastructure as Code (IaC)**
- **Workflow orchestration**
- **Cloud-native data lakes**
- **Scalable warehouse modeling**
- **Data visualization**

---

# 🏗️ Architecture

The pipeline follows a structured flow from raw data ingestion to analytics:

1. **Extract**  
   Python scripts fetch climate data from **Our World in Data**.

2. **Load (Data Lake)**  
   Raw CSV files are stored in **Google Cloud Storage (GCS)**.

3. **Stage (Data Warehouse)**  
   Data is loaded into **BigQuery staging tables** for preprocessing.

4. **Transform**  
   SQL transformations aggregate and model the data into optimized **analytics tables**.

5. **Visualize**  
   Insights are presented through an interactive **Looker Studio dashboard**.

---

# 🛠️ Technology Stack

| Layer | Technology |
|------|------------|
| Infrastructure as Code | Terraform |
| Cloud Platform | Google Cloud Platform (GCP) |
| Data Lake | Google Cloud Storage (GCS) |
| Orchestration | Bruin |
| Batch Processing | Bruin |
| Data Warehouse | BigQuery |
| Visualization | Looker Studio |
| Programming Language | Python |
| Transformations | SQL (BigQuery Dialect) |

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
climate-data-platform/
│
├── terraform/          # Infrastructure as Code for GCS and BigQuery
├── pipelines/          # Bruin workflow orchestration
├── ingestion/          # Python scripts for downloading datasets
├── warehouse/          # SQL transformations (staging + marts)
├── dashboards/         # Looker Studio dashboard assets
├── scripts/            # Utility and automation scripts
└── requirements.txt    # Python dependencies
```
---

## 🚀 Setup & Execution

### 1. Clone the Repository
```bash 
git clone [https://github.com/](https://github.com/)<your-username>/climate-data-platform
cd climate-data-platform
```

### 2. Configure Google Cloud

Authenticate and set your active project:
```bash 
gcloud auth application-default login 
gcloud config set project <project-id>
```

### 3. Provision Infrastructure

Use Terraform to create the Data Lake (GCS) and Warehouse (BigQuery):
```bash 
cd terraform
terraform init 
terraform apply
```

### 4. Install Dependencies
```bash 
pip install -r requirements.txt
```

### 5. Run the Pipeline

The pipeline is orchestrated by **Bruin** and runs on a daily batch schedule at **02:00 AM**. To run manually:
```bash 
# Ingest data python ingestion/download_temperature.py python ingestion/download_sea_level.py
# Upload to GCS gsutil cp *.csv gs://<bucket>/raw/
# Execute SQL transformations in BigQuery
```

### 6. Build the Dashboard

1. Open **Looker Studio** and connect to **BigQuery**.
2. Select the `climate_dw` dataset.
3. Create charts using `fact_temperature` and `fact_sea_level`.

---

## 📈 Analytics Dashboard

The dashboard provides two primary analytical views:

* **Global Temperature Trend**: A line chart showing annual temperature anomalies.
* **Sea Level Change**: A line chart visualizing long-term rising sea levels.

---

## 🔮 Future Improvements

* Implementation of **Data Quality Validation** checks.
* **Incremental data loading** to optimize processing costs.
* **CI/CD pipeline** integration for Terraform and SQL deployments.
* Automated **Schema Detection** and **Data Catalog** documentation.

---

**Author**: Data Engineering Portfolio Project
