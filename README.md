# Healthcare Data Pipeline

> A scalable, production-ready data pipeline for processing and analyzing large-scale healthcare datasets using Apache Spark, AWS S3, and Streamlit.

[![Python](https://img.shields.io/badge/Python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![PySpark](https://img.shields.io/badge/PySpark-3.0+-orange.svg)](https://spark.apache.org/)
[![AWS S3](https://img.shields.io/badge/AWS-S3-yellow.svg)](https://aws.amazon.com/s3/)
[![Streamlit](https://img.shields.io/badge/Streamlit-1.0+-red.svg)](https://streamlit.io/)

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Features](#features)
- [Technology Stack](#technology-stack)
- [Dataset](#dataset)
- [Getting Started](#getting-started)
- [Pipeline Components](#pipeline-components)
- [Dashboard Preview](#dashboard-preview)
- [Project Structure](#project-structure)
- [Configuration](#configuration)
- [Performance Optimization](#performance-optimization)
- [Key Learnings](#key-learnings)
- [Contributing](#contributing)
- [License](#license)
- [Contact](#contact)

---

## Overview

This project implements a comprehensive end-to-end data pipeline designed to handle massive healthcare datasets with efficiency and scalability. By leveraging the power of distributed computing with Apache Spark on AWS EMR and the simplicity of Streamlit for visualization, we have created a robust solution for healthcare data analytics.

### Key Highlights

- **10+ GB** of Medicare Part D prescription data processed
- **25+ Million** rows of prescriber-drug interactions analyzed
- **Three-tier data architecture** ensuring data quality and accessibility
- **Interactive dashboards** for real-time insights
- **Cloud-native design** leveraging AWS infrastructure

---

## Architecture

Our architecture follows modern data engineering best practices with a clear separation of concerns across ingestion, processing, storage, and visualization layers.

![Architecture Diagram](https://github.com/arpittomar246/Healthcare-data-pipeline/blob/main/images/architecture.jpg)

### Architecture Components

#### 1. Data Source Layer

- **PostgreSQL Database** containing raw healthcare data
- Four primary tables with relationships
- Approximately 10GB of structured data

#### 2. Processing Layer - Apache Spark on EMR

The ETL pipeline consists of four critical stages:

- **Ingest**: Raw data extraction from PostgreSQL
- **Validate & Clean**: Data quality checks and standardization
- **Transform**: Business logic and feature engineering
- **Publish**: Optimized data writing to storage

#### 3. Data Lake - Amazon S3

A three-zone medallion architecture:

- **Raw Zone**: Unprocessed data as ingested
- **Cleansed Zone**: Validated and standardized data
- **Curated Zone**: Analytics-ready transformed data

#### 4. Visualization Layer - Streamlit

- Interactive dashboards with real-time filtering
- Direct S3 data access for optimal performance
- Python-native analytics and plotting

---

## Features

### Data Processing

- **Distributed Computing**: Leverages Spark's parallel processing capabilities
- **Data Quality**: Built-in validation and cleansing mechanisms
- **Incremental Processing**: Support for batch and incremental loads
- **Partitioning Strategy**: Optimized data partitioning for query performance
- **Schema Evolution**: Handles changes in source data structure

### Analytics & Visualization

- **Drug Analytics Dashboard**: Top prescribed medications analysis
- **Prescriber Dashboard**: Healthcare provider insights
- **Trend Analysis**: Historical prescription patterns
- **Geographic Insights**: State-wise distribution analysis
- **Interactive Filtering**: Dynamic data exploration

### DevOps & Operations

- **Local Development Mode**: Test pipeline without cloud resources
- **Comprehensive Logging**: Application and Spark logs
- **Configuration Management**: Externalized configuration files
- **Testing Support**: Unit and integration test frameworks
- **JAR Management**: JDBC driver handling for database connections


## Dashboard Overview

![Top drug share](https://github.com/arpittomar246/Healthcare-data-pipeline/blob/main/images/reports%20and%20live%20stats.png)

**Key Insights:**
- Top prescribers by specialty
- Geographic distribution
- Prescription patterns
- Provider performance metrics

---

## Technology Stack

| Layer | Technology | Purpose |
|-------|-----------|---------|
| **Data Source** | PostgreSQL | Relational database for raw data |
| **Processing** | Apache Spark (PySpark) | Distributed data processing |
| **Compute** | AWS EMR | Managed Spark cluster |
| **Storage** | Amazon S3 | Scalable object storage |
| **Orchestration** | Python | Pipeline orchestration |
| **Visualization** | Streamlit | Interactive dashboards |
| **Development** | Pandas, NumPy | Local data analysis |

---

## Dataset

### Source

Data sourced from [CMS (Centers for Medicare & Medicaid Services)](https://data.cms.gov/provider-summary-by-type-of-service) - Medicare Part D Prescriber dataset.

### Schema Overview

| Table | Rows | Description |
|-------|------|-------------|
| **prescriber_drug** | ~25M | Prescription records linking prescribers to drugs |
| **prescriber** | ~1.1M | Healthcare provider information |
| **drug** | ~115K | Medication details and classifications |
| **state** | ~30K | Geographic reference data |

### Data Volume

- **Total Size**: Approximately 10 GB
- **Time Period**: Multi-year Medicare prescription data
- **Coverage**: Nationwide prescriber and drug information

---

## Getting Started

### Prerequisites

- Python 3.8 or higher
- Apache Spark 3.0+ (for local development)
- AWS Account (for production deployment)
- PostgreSQL database
- Git

### Installation

#### 1. Clone the repository

```bash
git clone https://github.com/arpittomar246/Healthcare-data-pipeline.git
cd Healthcare-data-pipeline
```

#### 2. Create virtual environment

**Windows:**
```bash
python -m venv venv
.venv\Scripts\Activate.ps1
```

**Linux/Mac:**
```bash
python3 -m venv venv
source venv/bin/activate
```

#### 3. Install dependencies

```bash
pip install -r requirements.txt
```

#### 4. Configure database connection

```bash
# Edit utils/project.cfg with your database credentials
cp utils/project.cfg.example utils/project.cfg
```

### Quick Start - Local Mode

Run the complete pipeline locally using sample data:

```bash
# Run exploratory data analysis
python eda.py --raw-dir local_data/raw --out-dir local_data/artifacts

# Execute the pipeline
python -m src.runnerfile --config utils/project.cfg --force-fresh --skip-anonymize

# Launch Streamlit dashboard
streamlit run run_dashboard.py

# Alternative: Launch GUI
python run_gui.py
```

### Production Deployment

For AWS EMR deployment:

```bash
# Configure AWS credentials
aws configure

# Submit Spark job to EMR cluster
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --conf spark.executor.memory=8g \
  --conf spark.executor.cores=4 \
  src/main.py
```

---

## Pipeline Components

### 1. Ingestion Module

Extract data from PostgreSQL and write to S3 Raw zone:
- Read from source tables
- Apply basic schema validation
- Write to Parquet format
- Partition by date/region

### 2. Cleansing Module

Data quality and standardization:
- Remove duplicates
- Handle null values
- Standardize data types
- Validate referential integrity
- Apply business rules

### 3. Transformation Module

Business logic and feature engineering:
- Join multiple datasets
- Calculate metrics (prescription counts, costs)
- Aggregate by dimensions
- Create derived features
- Apply windowing functions

### 4. Publishing Module

Optimize for analytics:
- Write to curated zone
- Create optimized file formats
- Generate summary tables
- Update data catalog

---

## Dashboard Preview

### Drug Analytics Dashboard

![Top drug by prescription](https://github.com/arpittomar246/Healthcare-data-pipeline/blob/main/images/top%20drug%20by%20prescriptions.png)

**Key Metrics:**
- Top prescribed medications by volume
- Drug cost analysis
- Generic vs Brand comparison
- Trend analysis over time

### Prescriber Analytics Dashboard

![Top drug share](https://github.com/arpittomar246/Healthcare-data-pipeline/blob/main/images/top%20drug%20share.png)

**Key Insights:**
- Top prescribers by specialty
- Geographic distribution
- Prescription patterns
- Provider performance metrics

---

## Project Structure

```
Healthcare-data-pipeline/
│
├── src/                          # Source code
│   ├── ingestion/               # Data ingestion modules
│   ├── cleansing/               # Data cleaning logic
│   ├── transformation/          # Business transformations
│   ├── publishing/              # Output generation
│   ├── utils/                   # Helper functions
│   └── runnerfile.py           # Main pipeline orchestrator
│
├── local_data/                  # Local development data
│   ├── raw/                    # Raw CSV files
│   └── artifacts/              # Generated outputs
│
├── utils/                       # Configuration and utilities
│   ├── project.cfg             # Pipeline configuration
│   └── logging.conf            # Logging configuration
│
├── dashboards/                  # Streamlit dashboard code
│   ├── drug_report.py
│   └── prescriber_report.py
│
├── tests/                       # Test cases
│   ├── unit/
│   └── integration/
│
├── notebooks/                   # Jupyter notebooks for EDA
│
├── docs/                        # Documentation
│
├── eda.py                       # Exploratory data analysis
├── run_dashboard.py             # Streamlit launcher
├── run_gui.py                   # GUI launcher
├── requirements.txt             # Python dependencies
└── README.md                    # This file
```

---

## Configuration

### Pipeline Configuration

Edit `utils/project.cfg` with your configuration:

```ini
[DATABASE]
host = your-postgres-host
port = 5432
database = healthcare_db
user = your-username
password = your-password

[S3]
bucket = your-s3-bucket
raw_zone = raw/
cleansed_zone = cleansed/
curated_zone = curated/

[SPARK]
app_name = HealthcarePipeline
executor_memory = 8g
executor_cores = 4
driver_memory = 4g
```

### Environment Variables

```bash
export AWS_ACCESS_KEY_ID=your-access-key
export AWS_SECRET_ACCESS_KEY=your-secret-key
export AWS_DEFAULT_REGION=us-east-1
export SPARK_HOME=/path/to/spark
```

---

## Performance Optimization

### Spark Tuning Strategies

#### 1. Partitioning

- Optimal partition size: 128MB - 256MB
- Partition by date and region for better pruning
- Use coalesce/repartition strategically

#### 2. Memory Management

```python
spark.executor.memory = 8g
spark.driver.memory = 4g
spark.memory.fraction = 0.8
```

#### 3. Caching

- Cache frequently accessed DataFrames
- Use appropriate storage levels
- Clear cache when no longer needed

#### 4. Broadcast Joins

- Broadcast small dimension tables (less than 10MB)
- Reduces shuffle operations
- Improves join performance

### Resource Optimization

| Cluster Size | Executors | Memory per Executor | Cores per Executor | Total Cores |
|-------------|-----------|--------------------|--------------------|-------------|
| **Small** | 4 | 8 GB | 4 | 16 |
| **Medium** | 8 | 8 GB | 4 | 32 |
| **Large** | 16 | 8 GB | 4 | 64 |

---
## Dashboard report Overview

![Top drug share](https://github.com/arpittomar246/Healthcare-data-pipeline/blob/main/images/Automated%20eda%20report.png)

![Top drug share](https://github.com/arpittomar246/Healthcare-data-pipeline/blob/main/images/Readable%20reports.png)





## Key Learnings

### Technical Skills Developed

- **Spark Fundamentals**: Deep understanding of RDDs, DataFrames, and Spark SQL
- **Performance Tuning**: Resource optimization and query optimization techniques
- **Cloud Architecture**: AWS service integration and best practices
- **Data Quality**: Implementing validation and cleansing frameworks
- **Pipeline Orchestration**: End-to-end workflow management
- **Monitoring & Logging**: Application observability and debugging

### Best Practices Implemented

- **Modular Code Design**: Separation of concerns with reusable components
- **Configuration Management**: Externalized configuration for flexibility
- **Error Handling**: Robust exception handling and recovery mechanisms
- **Testing**: Unit and integration tests for reliability
- **Documentation**: Comprehensive code and architecture documentation

---

## Contributing

We welcome contributions! Please follow these guidelines:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

### Development Guidelines

- Follow PEP 8 style guide for Python code
- Add unit tests for new features
- Update documentation as needed
- Ensure all tests pass before submitting PR

---

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

## Authors

**Author1**: Mahima Yadav (G24AI2027)  
**Author2**: Pallavi Sarangi (G24AI2091)
**Author3**: Muraliedhar Kanchibhotla (G24AI2014)
**Author4**: Arpit Tomar (G24AI2001)

**Project Link**: [Healthcare Data Pipeline](https://github.com/arpittomar246/Healthcare-data-pipeline)


---

## Acknowledgments

- **CMS** for providing comprehensive Medicare datasets
- **Apache Spark Community** for excellent documentation
- **AWS** for robust cloud infrastructure
- **Streamlit** for making data visualization accessible

---

## Future Enhancements

- Real-time streaming pipeline using Spark Streaming
- Machine Learning models for prescription prediction
- Advanced anomaly detection in prescription patterns
- API development for external data access
- Docker containerization for easy deployment
- CI/CD pipeline integration
- Data lineage and catalog implementation
- Enhanced security with data encryption

---

<div align="center">

**Made with care for Healthcare Analytics**

Star this repository if you find it helpful!

</div>
