# SQL Data Warehouse Project

**Building a Modern Data Warehouse with SQL Server – ETL, Data Modeling & Analytics**

---

## 📌 Project Overview

Welcome to the **SQL Data Warehouse Project** repository. This project demonstrates a complete **data warehouse and analytics solution** using SQL Server. It covers the full lifecycle of analytical data engineering and business intelligence:

- Ingesting and transforming raw data (ETL)
- Designing star schema models
- Performing Exploratory Data Analysis (EDA)
- Conducting advanced analytics to derive business insights
- Creating analytical reporting outputs

The structure and analytics align with real-world industry practices used by data engineers, business intelligence analysts, and data analysts.

---

## 🏗️ Data Architecture

This project uses a **multi-layered analytics architecture** to organize and optimize data:

- **Bronze Layer** — Raw source data ingested as is
- **Silver Layer** — Cleansed and normalized data
- **Gold Layer** — Business-ready data models optimized for analytics

This layered architecture ensures data quality, consistency, and performance for analytical workloads.

---

## 🔍 Key Components

### 📁 Database & Schema
- **Fact Table**
  - `gold.fact_sales`: Sales transactions with measures such as `sales_amount`, `quantity`, and `price`.
- **Dimension Tables**
  - `gold.dim_customers`: Customer demographics
  - `gold.dim_products`: Product attributes

---

## 🔎 Exploratory Data Analysis (EDA)

EDA is performed using SQL to understand:

- Structure and content of tables
- Key descriptive metrics (e.g., total sales, customer count, product count)
- Business KPI summaries
- Distribution of data by country, gender, product category
- Ranking of products by performance

EDA queries help answer foundational business questions such as:

- Who are the top customers?
- What products generate the most revenue?
- How is product distribution across categories?

---

## 📈 Advanced Analytics

This section applies analytical SQL techniques to derive deeper insights:

### Trend Analysis
- Yearly and monthly sales performance
- Customer growth over time

### Cumulative & Moving Averages
- Running total of sales per month
- Moving averages to identify patterns

### Performance Comparison
- Product performance over multiple years
- Trend analysis relative to historical averages

### Segment Analysis
- Product cost ranges
- Customer segmentation based on spend and lifespan

### Part-to-Whole Analysis
- Contribution of categories to total revenue

---

## 📊 Reporting Views

To support dashboards and business reporting, two analytical views are created:

### `gold.report_customers`
Outputs detailed customer metrics:
- Total orders
- Total sales
- Customer segments (VIP, Regular, New)
- Average order values

### `gold.report_products`
Outputs product performance metrics:
- Total sales and quantity
- Product segments (High, Mid, Low performers)
- Average selling price and monthly revenue

These views provide ready-to-use data for dashboards and executive reporting.

---

## 🎯 Business Questions Addressed

This project helps answer strategic business questions including:

- Which products drive the highest revenue?
- How has sales performance changed over time?
- Who are the most valuable and high-value customers?
- What are the trends in customer acquisition?
- How does product performance vary across categories?

The SQL analytics techniques demonstrated here are applicable to **data analyst**, **business intelligence**, and **analytics engineering** use cases.

---

## 🛠️ Technologies Used

- **Database**: SQL Server / T-SQL
- **Analytics**: SQL queries, aggregations, window functions
- **Data Modeling**: Star schema design
- **Reporting**: SQL views for analytics

---

---

## 👤 About Me

**Vaibhav Tiwari**  
Final-year student at **Punjab Engineering College**  
I am passionate about data analytics, data engineering, and business intelligence. Through this project, I demonstrate my ability to build analytical systems that transform raw data into insights using SQL, data modeling, and structured analytics.

---


