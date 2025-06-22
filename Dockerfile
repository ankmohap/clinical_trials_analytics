FROM apache/airflow:latest-python3.12

# Set the working directory
WORKDIR /opt/airflow

# Copy the requirements.txt file to the container
COPY requirements.txt ./

# Check if the requirements.txt was copied
RUN ls -al /opt/airflow

# Install the required Python packages from the requirements.txt file
RUN pip install --no-cache-dir -r requirements.txt

# Check that pip packages are installed
RUN pip list