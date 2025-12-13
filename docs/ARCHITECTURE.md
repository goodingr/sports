# System Architecture

This document provides a high-level overview of the system architecture, including data ingestion, machine learning pipeline, and user interfaces.

```mermaid
graph TD
    %% Define Styles
    classDef pipeline fill:#f9f,stroke:#333,stroke-width:2px;
    classDef storage fill:#ff9,stroke:#333,stroke-width:2px;
    classDef service fill:#9cf,stroke:#333,stroke-width:2px;
    classDef frontend fill:#9f9,stroke:#333,stroke-width:2px;
    classDef external fill:#ddd,stroke:#333,stroke-width:2px;

    subgraph External_World ["External World"]
        DataSources["External Data Sources<br/>(Odds API, ESPN, NBA API)"]:::external
        Users[End Users]:::external
        Analysts[Analysts / Developers]:::external
    end

    subgraph Orchestration ["Automation & Pipeline (Powershell)"]
        Pipeline["pipeline.ps1<br/>(Master Orchestrator)"]:::pipeline
        IngestScript["ingest_all.ps1"]:::pipeline
        TrainScript["train.ps1"]:::pipeline
        PredictScript["predict.ps1"]:::pipeline
        
        Pipeline --> IngestScript
        Pipeline --> TrainScript
        Pipeline --> PredictScript
    end

    subgraph Core_Logic ["Python Core (src/)"]
        IngestLogic["Data Ingestion<br/>(src/data)"]
        FeatureLogic["Feature Engineering<br/>(src/features)"]
        ModelLogic["Model Training<br/>(src/models)"]
        PredictLogic["Inference Engine<br/>(src/predict)"]
    end

    subgraph Storage_Layer ["Data Persistence"]
        SQLite[("SQLite Database<br/>(database.sqlite)")]:::storage
        Parquet[("Parquet Files<br/>(data/ & predictions/)")]:::storage
        ModelsFile[("Serialized Models<br/>(models/)")]:::storage
    end

    subgraph Serving_Layer ["Backend Services"]
        FastAPI["FastAPI Backend<br/>(src/api - run_api.ps1)"]:::service
        DashBoard["Dash Analytics<br/>(src/dashboard - run_dashboard)"]:::service
    end

    subgraph Frontend_Layer ["User Interface"]
        WebApp["Web App<br/>(Next.js / web-app)"]:::frontend
    end

    %% Data Flow
    IngestScript --> IngestLogic
    IngestLogic -->|Fetch| DataSources
    IngestLogic -->|Write| SQLite
    IngestLogic -->|Write| Parquet

    TrainScript --> ModelLogic
    ModelLogic -->|Read| SQLite
    ModelLogic -->|Read| Parquet
    ModelLogic -->|Process| FeatureLogic
    ModelLogic -->|Save| ModelsFile

    PredictScript --> PredictLogic
    PredictLogic -->|Load| ModelsFile
    PredictLogic -->|Read| SQLite
    PredictLogic -->|Generate| FeatureLogic
    PredictLogic -->|Write| SQLite
    PredictLogic -->|Write| Parquet

    %% Serving Flow
    FastAPI -->|Query| SQLite
    DashBoard -->|Analyze| Parquet
    DashBoard -->|Analyze| SQLite

    %% User Interaction
    WebApp -- "API Calls (HTTP)" --> FastAPI
    Users -->|Interact| WebApp
    Analysts -->|Monitor| DashBoard
```

## Component Overview

### 1. Automation & Pipeline
The system is orchestrated by PowerShell scripts located in the `scripts/` directory.
- **pipeline.ps1**: The master entry point that coordinates ingestion, training, and prediction.
- **ingest_all.ps1**: Handles data fetching from external sports APIs.
- **train.ps1**: Retrains machine learning models (LightGBM, XGBoost).
- **predict.ps1**: Runs the inference engine to generate betting probabilities.

### 2. Core Logic (`src/`)
- **src/data**: Connectors for data sources (NFL, NBA, etc.).
- **src/features**: Logic for transforming raw stats into model features (rolling averages, differentials).
- **src/models**: Configuration and training logic for ML models.
- **src/predict**: The runtime engine that applies models to upcoming games.

### 3. Data Storage
- **database.sqlite**: Stores structured relational data (game schedules, odds, team info).
- **Parquet Files**: Stores large tabular datasets, including historical features (`data/`) and model predictions (`data/forward_test`), for efficient reading.

### 4. Serving & Frontend
- **FastAPI**: A lightweight Python API server (`run_api.ps1`) that exposes prediction data to the frontend.
- **Dash**: An internal analytics dashboard (`run_dashboard`) for visualizing model performance and backtesting results.
- **Next.js Web App**: The user-facing application (`web-app/`) providing a modern interface for users to view betting insights.
