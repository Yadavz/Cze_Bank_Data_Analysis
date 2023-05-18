## Czechoslovakia Banking Financial Data Analysis

### Project Description
This project focuses on analyzing financial data provided by the Czechoslovakia Bank for the past 5 years. The dataset contains information on various aspects of the bank's operations, including accounts, cards, clients, dispositions, districts, loans, orders, and transactions. The goal of this analysis is to gain insights into the bank's performance, customer demographics, account types, card usage, expenses, loan portfolio, customer satisfaction, and potential areas for improvement. By utilizing Snowflake for data management and Power BI for visualization, this project aims to provide stakeholders with actionable insights to enhance decision-making.

### Project Steps
1. Data Collection: The dataset in CSV format is uploaded to an S3 bucket in AWS for storage.
2. Data Preparation: The data is collected from the S3 bucket using Snowflake. Tables and pipelines are created in Snowflake to connect with S3 and fetch the required data.
3. Data Cleaning: The cleaning process is initiated, ensuring data integrity and consistency. Any additional information provided by the client is incorporated using SQL queries to generate meaningful insights.
4. Data Analysis: Utilizing SQL queries and analysis techniques, various aspects of the dataset are explored to uncover valuable insights.
5. Data Visualization: Power BI is employed to create visualizations, charts, and dashboards that highlight the key findings from the analysis, enabling stakeholders to quickly grasp important information and make informed decisions.

### Dataset Overview
The dataset consists of the following tables:
1. Account: Contains information about bank accounts, including account ID, opening date, associated client ID, and account type.
2. Card: Provides details about bank cards, including card ID, issuance date, and card type.
3. Client: Contains information about bank clients, including client ID, birthdate, gender, and district of residence.
4. Disposition: Represents the relationship between clients and their accounts, including disposition ID, associated client ID, and disposition type.
5. District: Contains information about various districts in Czechoslovakia, including district ID, district name, and demographic and economic indicators.
6. Loan: Contains details about loans issued by the bank, including loan ID, issuance date, associated account ID, and loan amount.
7. Order: Provides information about orders issued by bank clients, including order ID, associated account ID, issuance date, and order description.
8. Transaction: Contains information about transactions made by bank clients, including transaction ID, associated account ID, transaction date, transaction type, and transaction amount.

### Insights and Visualizations
- Demographic Profile: Analyzed the client data to understand the demographic profile of bank clients and variations across districts.
- Bank Performance: Conducted a year and month-wise analysis to evaluate the bank's performance over the years, highlighting key trends and patterns.
- Account Types: Explored the most common types of accounts and compared their usage and profitability.
- Card Usage: Examined the frequency of card types used by bank clients and assessed the overall profitability of the credit card business.
- Expense Analysis: Identified the major expenses incurred by the bank and proposed strategies to reduce expenses and improve profitability.
- Loan Portfolio: Analyzed the bank's loan portfolio, investigating variations across different loan purposes and client segments.
- Customer Service Improvement: Suggested measures to enhance customer service and improve satisfaction levels.
- New Product Opportunities: Explored the potential introduction of new financial products or services to attract more customers and increase profitability.

### Project Components
The repository includes the following files and folders:
- `data/`: Contains the dataset files in CSV format.
- `scripts/`: Contains the SQL scripts used for data preparation, cleaning, and analysis.
- `documentation/`: Includes any relevant documentation related to the project.
- `power_bi/`: Contains the Power BI file (.pbix) used for data visualization and creating the dashboard.


### Installation and Usage
1. Clone or download the repository to your local machine.
2. Set up a Snowflake account and configure the necessary credentials to connect to Snowflake.
3. Create the required tables and pipelines in Snowflake to import the data from the S3 bucket.
4. Execute the SQL scripts provided in the `scripts/` folder to perform data cleaning and analysis.
5. Install Power BI and open the Power BI file (`power_bi/financial_analysis.pbix`) to access the visualizations and dashboard.
6. Interact with the visualizations to explore the insights and make informed decisions.

### Results and Recommendations
Based on the analysis conducted, the project yielded the following results and recommendations:
- The bank's performance has shown steady growth over the past 5 years, with an increase in the number of clients and transactions.
- The most common account types are savings and current accounts, contributing significantly to the bank's profitability.
- Credit cards have gained popularity among clients, with frequent usage observed, presenting an opportunity for revenue growth.
- By reducing unnecessary expenses, such as operational costs, the bank can improve profitability.
- Loan portfolios should be diversified, considering different loan purposes and client segments, to mitigate risks and increase revenue streams.
- Improving customer service by streamlining processes and enhancing communication can boost customer satisfaction and retention.
- Exploring new product opportunities, such as investment services or digital banking solutions, can attract more clients and increase competitiveness.

