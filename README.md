# GCP E-Commerce Data Engineering Pipeline

##  Project Overview

An end-to-end e-commerce data engineering pipeline built using Google Cloud Platform.

The pipeline ingests daily order data from Google Cloud Storage, orchestrates processing with Cloud Composer (Apache Airflow), transforms and validates data in BigQuery using Bronze, Silver, and Gold layers, and presents business insights through Google Data Studio.

## 🏗️ Architecture


                         ┌──────────────────────┐
                         │   Daily Orders CSV   │
                         └──────────┬───────────┘
                                    │
                                    ▼
                         ┌──────────────────────┐
                         │    Cloud Storage     │
                         │      Raw Layer       │
                         └──────────┬───────────┘
                                    │
                                    ▼
                         ┌──────────────────────┐
                         │   Cloud Composer     │
                         │     Apache Airflow   │
                         └──────────┬───────────┘
                                    │
                                    ▼
                         ┌──────────────────────┐
                         │ BigQuery - Staging   │
                         └──────────┬───────────┘
                                    │
                              Validation
                                    │
                                    ▼
                         ┌──────────────────────┐
                         │ BigQuery - Bronze    │
                         │   Incremental MERGE  │
                         └──────────┬───────────┘
                                    │
                                    ▼
                         ┌──────────────────────┐
                         │ BigQuery - Silver    │
                         │ Cleaned / Typed Data │
                         └──────────┬───────────┘
                                    │
                                    ▼
                         ┌──────────────────────┐
                         │ BigQuery - Gold      │
                         │ Business Aggregates  │
                         └──────────┬───────────┘
                                    │
                                    ▼
                         ┌──────────────────────┐
                         │     Data Studio      │
                         │     Dashboard        │
                         └──────────────────────┘
