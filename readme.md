# PySpark Tutorial Practice

This repository contains my hands-on practice and notes from the Udemy course: **PySpark tutorial with 40+ hands-on examples of analyzing large data sets on your desktop or on Hadoop with Python!**


## 🎯 Purpose

The goal of this repository is to consolidate my learning by practicing code examples from the course, section by section.


## 📘 Course Overview

The course covers a wide range of PySpark topics and includes numerous hands-on examples for analyzing large datasets either locally or on a Hadoop cluster. It focuses on practical application and building familiarity with PySpark's core APIs and components.


## 📌 Notes

This repository is for educational and personal learning purposes only. All code is written by me while following along with the course material, with some custom additions where applicable.


## 📜 License

This project is open for learning and sharing. Respect the original course and instructor’s rights.


## 📂 Repository Structure

Each folder or script in this repo corresponds to a specific topic, section, or exercise from the course. Topics include but are not limited to :
- PySpark Basics
- DataFrames and SQL
- RDD Operations
- Data Cleaning and Transformation
- Working with Hadoop (optional parts)
- Real-world Data Analysis Examples


## 🛠 Requirements

The examples in this repository were tested using the following versions:

- 🐍 **Python** : 3.10.10  
- ⚙️ **Apache Spark** : 3.4.4  
- ☕ **Java (JDK)** : 17.0.10


## 💻 Environment Setup

The working environment was prepared following the instructions provided at : 
[https://www.sundog-education.com/spark-python/](https://www.sundog-education.com/spark-python/)

### 1️⃣ Install Java Development Kit (JDK)

- Download and install JDK version **17.0.10** from : [https://www.oracle.com/java/technologies/downloads/](https://www.oracle.com/java/technologies/downloads/)

### 2️⃣ Download Apache Spark

- Link : [https://spark.apache.org/downloads.html](https://spark.apache.org/downloads.html)
- Spark release **3.4.4**,
- Package type : **Pre-built for Apache Hadoop 3.3 and later**

### 3️⃣ Extract Resources and Set Environment Variables

You can use the following PowerShell script to automatically extract the resources and configure your system environment :

<details>
<summary>📜 Click to expand PowerShell setup script</summary>

```powershell

# ---------------------------------------------
# Configure paths and resource names
# ---------------------------------------------

# Define resource names
$hadoop_name = "hadoop"
$spark_name = "spark-3.4.4-bin-hadoop3"

# Define paths to the compressed resources
$hadoop_archive_path = "~\Downloads\$hadoop_name.zip"
$spark_archive_path  = "~\Downloads\$spark_name.tgz"

# Define target extraction directories
$hadoop_target_path = "C:\Hadoop"
$spark_target_path  = "C:\Spark"
$jdk_path = "C:\Program Files\Java\jdk-17"

# Create a unique temporary staging folder for safe extraction
$extract_stage_path = "temp_$(Get-Date -Format 'yyyyMMdd_HHmmss')"

# Environment variables names
$PYSPARK_PYTHON = "PYSPARK_PYTHON"
$HADOOP_HOME = "HADOOP_HOME"
$SPARK_HOME = "SPARK_HOME"
$JAVA_HOME = "JAVA_HOME"


# ---------------------------------------------
# Ensure output directories exist
# ---------------------------------------------
Write-Output "Creating root directories ..."
# Ensure root output directories exist
New-Item -ItemType Directory -Path $extract_stage_path -Force | Out-Null
New-Item -ItemType Directory -Path $hadoop_target_path -Force | Out-Null
New-Item -ItemType Directory -Path $spark_target_path -Force | Out-Null


# ---------------------------------------------
# Extract and move Hadoop files
# ---------------------------------------------
Write-Output "Extracting Hadoop ..."
# Extract Hadoop archive to the staging folder
tar -xf $hadoop_archive_path -C $extract_stage_path
# Move Hadoop contents (excluding top-level folder) to the final destination
Move-Item -Path "$extract_stage_path\$hadoop_name\*" -Destination $hadoop_target_path


# ---------------------------------------------
# Extract and move Spark files
# ---------------------------------------------
Write-Output "Extracting Spark ..."
# Extract Spark archive to the same staging folder
tar -xf $spark_archive_path -C $extract_stage_path
# Move Spark contents (excluding top-level folder) to the final destination
Move-Item -Path "$extract_stage_path\$spark_name\*" -Destination $spark_target_path


# ---------------------------------------------
# Clean up the temporary extraction folder
# ---------------------------------------------
Write-Output "Cleaning up temporary files ..."
# Remove the temporary staging folder and its contents
Remove-Item -Path $extract_stage_path -Recurse -Force


# ---------------------------------------------
# Set environment variables
# ---------------------------------------------
Write-Output "Setting environment variables ..."
[System.Environment]::SetEnvironmentVariable($HADOOP_HOME, $hadoop_target_path, 'User')
[System.Environment]::SetEnvironmentVariable($SPARK_HOME, $spark_target_path, 'User')
[System.Environment]::SetEnvironmentVariable($JAVA_HOME, $jdk_path, 'User')
[System.Environment]::SetEnvironmentVariable($PYSPARK_PYTHON, 'Python', 'User')

# Append bin directories to the user PATH
$user_paths = [System.Environment]::GetEnvironmentVariable('Path', 'User')
if ($user_paths -notlike "*%$HADOOP_HOME%\bin*") { $user_paths = $user_paths + "%$HADOOP_HOME%\bin;" }
if ($user_paths -notlike "*%$SPARK_HOME%\bin*") { $user_paths = $user_paths + "%$SPARK_HOME%\bin;" }
[System.Environment]::SetEnvironmentVariable('Path', $user_paths, 'User')

```
</details>