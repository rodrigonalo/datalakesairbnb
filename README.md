# 🌍 RentScape Project Repository 🌍

This repository contains the codebase for **RentScape**, a data-powered visualization application that integrates diverse datasets into an Azure-based storage solution. The project focuses on Airbnb listings in the cities of **Prague** and **Barcelona**.

## 🚀 Repository Structure 🚀

The repository is divided into two main folders representing the two primary stages of the project:

### 📂 Folder: `dl_datalake/`
This folder contains all the Jupyter notebooks related to the **Data Lake** phase of the project. The files are organized in the correct execution order, with numbered prefixes for clarity. Each notebook corresponds to the extraction of data from a specific source or transformation steps required to prepare the data for the next phase.

#### Contents:
1. **`01_dl__extract___insideairbnb.ipynb`**: Extracts data from the InsideAirbnb dataset.
2. **`02_dl__extract___currency_nbp_api.ipynb`**: Fetches currency exchange data from the National Bank of Poland API.
3. **`03_dl__extract___open_ai.ipynb`**: Retrieves data via the OpenAI API.
4. **`04_dl__extract___overpass_api.ipynb`**: Extracts data using the Overpass API from OpenStreetMap.

---

### 📂 Folder: `dwh_datawarehouse/`
This folder contains all the Jupyter notebooks related to the **Data Warehouse** phase of the project. Similar to the Data Lake phase, the files are organized in the order they should be executed and correspond to different transformations, loads, and queries in the Data Warehouse.

#### Contents:
1. **`01_dwh__extract___insideairbnb.ipynb`**: Transformation for the InsideAirbnb dataset.
2. **`02_dwh__transform___currency_nbp.ipynb`**: Transformation of currency data for the warehouse.
3. **`03_dwh__transform___open_ai.ipynb`**: Transformation of OpenAI data for the warehouse.
4. **`04_dwh__transform___overpass_api.ipynb`**: Transformation for the Overpass API.
5. **`05_dwh__load___all_files_to_azure_sql.ipynb`**: Loads transformed data into the Azure SQL database.

---

### 🧑‍💻 Author Contributions 🧑‍💻

All team members contributed to both the **Data Lake** and **Data Warehouse** phases. The specific responsibilities for each data source and transformation are:

- **Daniel**:
  - InsideAirbnb dataset: Extraction and transformation.
  - Currency data from NBP API: Extraction and transformation.

- **Rodrigo**:
  - OpenAI API: Extraction and transformation.

- **Victor**:
  - Overpass API (OpenStreetMap): Extraction and transformation.
  - Loading of cleansed data to Azure SQL database.

---

## 🔑 Security 🔑

To ensure security and protect sensitive information, all keys, tokens, and credentials have been removed from the codebase. Make sure to set the appropriate environment variables or use secure vault services when running the code.

---

## 🌟 Project Purpose 🌟

The **RentScape** project combines and integrates multiple data sources into a centralized Azure-based architecture. The application enables dynamic, data-driven insights into Airbnb rental trends for **Prague** and **Barcelona** through interactive visualizations.

### 🔑 Key Components 🔑

1. **Azure Storage**:
   - **Data Lake**: Stores raw and pre-processed data.
   - **Data Warehouse**: Optimized for querying and analytics.

2. **Datasets**:
   - **Dynamic APIs**:
     - Currency Data (NBP API)
     - OpenAI Data
     - Overpass API (OpenStreetMap)
   - **Static Sources**:
     - InsideAirbnb Dataset.

3. **Visualization Application**:
   - Built in **Power BI**.
   - Access the **[online version](https://app.powerbi.com/view?r=eyJrIjoiNWU1MDMwZWMtZjljOS00NTY2LWExOWEtZWFmOGEzNzk1N2VkIiwidCI6IjZhMmE2NjZjLTQ2MzktNDYzYS04ZTNhLTIxMjczZTVkOTAyOCJ9)** to explore dynamic dashboards and gain insights.

---

## 🛠 How to Use 🛠

1. Clone the repository:
   ```bash
   git clone https://github.com/your-repo/rentscape.git
