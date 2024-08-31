# Global Burger Chain Data Processing Project

## Overview

This project is designed to handle and process data for a global burger chain with approximately 50 stores each in India, the USA, and the UK. The objective is to manage order data, stream it using Apache Kafka, and process it using Databricks. The processed data is stored in both Azure Data Lake Storage (ADLS) and SQL Server, enabling efficient data analysis and business decision-making. A dashboard is created for visualizing key metrics and insights.

## Architecture

![Architecture](image.png)

The project's architecture includes the following key components:

1. **Data Sources**:
    - **Order Data**: Real-time streaming data generated from orders at stores in India, the USA, and the UK. This data is ingested through Apache Kafka.
    - **SQL Server Data**: Additional business data sourced directly from a SQL Server database.

2. **Data Streaming and Processing**:
    - **Apache Kafka**: Used for streaming order data in real-time from multiple stores.
    - **Databricks**: Consumes the streaming data from Kafka. Databricks performs the following tasks:
        - **Raw Data Storage**: Stores unprocessed raw data in Azure Data Lake Storage (ADLS) for archival and future analysis.
        - **Data Transformation**: Processes and cleans the data, then stores the transformed data into a SQL Server database for further analysis.

3. **Data Transformation and ETL**:
    - **Azure Data Factory (ADF)**: Used for extracting, transforming, and loading (ETL) data from SQL Server to the destination SQL Server. ADF manages complex data transformation pipelines efficiently.

4. **Data Storage**:
    - **Azure Data Lake Storage (ADLS)**: Stores raw data received directly from Databricks, providing scalable and cost-effective storage.
    - **SQL Server**: Acts as the destination for processed and transformed data. This is the primary source for analytical reporting and dashboard creation.

5. **Data Visualization**:
    - A dashboard is created using data from the destination SQL Server to visualize key business metrics, such as sales performance, order volume, and customer preferences across different regions.

## Technologies Used

- **Apache Kafka**: For real-time data streaming.
- **Databricks**: For data processing, transformation, and integration.
- **Azure Data Lake Storage (ADLS)**: For storing raw data.
- **SQL Server**: For storing transformed data and acting as a data source for the dashboard.
- **Azure Data Factory (ADF)**: For orchestrating data transformations and ETL processes.
- **Dashboard Tool**: Used for creating visual reports and insights (e.g., Power BI).

## Setup and Configuration

1. **Kafka Setup**: Configure Kafka to stream data from the order management systems in each of the stores.
2. **Databricks Configuration**: Set up Databricks workspaces, clusters, and notebooks to process the incoming Kafka streams.
3. **ADLS Setup**: Create containers and set appropriate access permissions for Databricks to store raw data.
4. **SQL Server Setup**: Configure databases and tables for storing transformed data.
5. **ADF Pipelines**: Create ADF pipelines to handle data transformation from the initial SQL Server to the destination SQL Server.
6. **Dashboard Creation**: Connect the dashboard tool to the destination SQL Server and create visualizations for real-time data insights.

## Contributing

Contributions are welcome! Please submit a pull request or open an issue for any improvements or feature requests.

## Contact

Feel free to explore the project and contribute to its development! If you have any questions or suggestions, don't hesitate to reach out - [LinkedIn](https://www.linkedin.com/in/yash-kothari-5727781b2/).
