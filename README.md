# FPL Data Engineering Pipeline | AWS-Snowflake project

## Introduction
This project, the "FPL Data Engineering Pipeline," demonstrates the construction of a robust and scalable data engineering solution using AWS cloud services and the Snowflake data warehouse. The pipeline is designed to process and analyze sports data, leveraging Python for data extraction and transformation. This project emphasizes the importance of automation, reliability, and performance in modern data engineering solutions.

## Architecture
![Project Architecture](https://github.com/Svenko333/FPL-data-engineering-AWS-Snowflake-project/blob/main/FPL%20Project.jpg)

## Technology Used
1. Programming Language - Python
2. Scripting Language - SQL
3. Cloud Platform - Amazon Web Services (AWS)
   - Lambda Function: A serverless compute service that is running a code for data extraction and data transformation
   - S3 Bucket: Object storage service used to store raw data and the transformed data
   - CloudWatch: Used to trigger the data extraction Lambda function on a daily schedule
   - Object Put Trigger: This mechanism triggers the data transformation Lambda function whenever a new file (raw data) is placed in the                           designated S3 bucket, it enables automated pipeline orchestration
4. Data Warehousing - Snowflake

## Dataset
Data source is the official FPL API. If you want to know more, you can check out an article on Medium that goes deeper on FPL API : https://medium.com/@frenzelts/fantasy-premier-league-api-endpoints-a-detailed-guide-acbd5598eb19

## Scripts for Project
1. <a href="https://github.com/Svenko333/FPL-data-engineering-AWS-Snowflake-project/blob/main/Data%20Extraction.py">Extract Python File</a>
2. <a href="https://github.com/Svenko333/FPL-data-engineering-AWS-Snowflake-project/blob/main/Data%20Transformation.py">Transform Python File</a>
3. <a href="https://github.com/Svenko333/FPL-data-engineering-AWS-Snowflake-project/blob/main/SQL%20-%20Snowflake%20part.sql">Load SQL File</a>



