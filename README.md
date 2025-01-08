# 🌍 RentScape 🌍

This repository contains the codebase for the **RentScape** project, a data-powered visualization application that integrates diverse datasets into an Azure-based storage solution. The project focuses on Airbnb listings in the cities of **Prague** and **Barcelona**.

## 🚀 Repository Structure 🚀

The repository is organized to reflect the contributions of individual team members. Each team member is responsible for a specific portion of the project, and their work is contained within folders named after them.

### 📂 Team Member Folders 📂

- **`daniel/`**
- **`rodrigo/`**
- **`victor/`**

Each folder includes the following subfolders:

1. **`dl_datalake/`**:
   - Contains code related to data extraction, transformation, and storage in the Azure-based data lake.
   - Files within this folder are named to indicate:
     - **The dataset they relate to.**
     - **The stage of the pipeline they correspond to.**

     For example:
     - `dl__extract___currency_nbp_api.ipynb`: Extracts currency exchange data from the National Bank of Poland API.
     - `dl__extract___inside_airbnb.ipynb`: Extracts Airbnb listing data from the Inside Airbnb dataset.

2. **`dwh_datawarehouse/`**:
   - Contains code responsible for the data warehouse layer. This is where transformed and processed data is stored for efficient querying and analysis.

### 🗂 Example Folder Breakdown 🗂

#### `daniel/`
- **`dl_datalake/`**:
  - `dl__extract___currency_nbp_api.ipynb`
  - `dl__extract___inside_airbnb.ipynb`
- **`dwh_datawarehouse/`**:
  - Code for structuring and querying the data warehouse.

The structure is mirrored in the folders for **Rodrigo** and **Victor**, with each team member focusing on specific datasets and project stages.

## 🌟 Project Purpose 🌟

The **RentScape Project** combines and integrates multiple data sources into a centralized Azure-based architecture. The application enables dynamic, data-driven insights into Airbnb rental trends for **Prague** and **Barcelona** through interactive visualizations.

### 🔑 Key Components 🔑

1. **Azure Storage**:
   - Data Lake: Stores raw and pre-processed data.
   - Data Warehouse: Optimized for querying and analytics.

2. **Datasets**:
   - **Currency Exchange Data**: Retrieved from the National Bank of Poland API.
   - **Airbnb Listings**: Extracted from the Inside Airbnb dataset.

3. **Visualization Application**:
   - Powered by interactive dashboards that allow users to explore rental trends, pricing patterns, and more.

## 🛠 How to Use 🛠

1. Clone the repository:
   ```bash
   git clone https://github.com/your-repo/rentscape.git
