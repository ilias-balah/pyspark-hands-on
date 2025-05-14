# ---------------------------------------------
# Configure paths and resource names
# ---------------------------------------------

# Define resource names
$hadoop_name = "hadoop"
$spark_name = "spark-3.4.4-bin-hadoop3"

# Define paths to the compressed resources
$hadoop_archive_path = "C:\Users\balah\Downloads\$hadoop_name.zip"
$spark_archive_path  = "C:\Users\balah\Downloads\$spark_name.tgz"

# Define target extraction directories
$hadoop_target_path = "C:\SH\Hadoop"
$spark_target_path  = "C:\SH\Spark"
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