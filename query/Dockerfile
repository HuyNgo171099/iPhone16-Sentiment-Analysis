FROM python:3.11-slim

# install GCC for package compatibility and clean up temp files early
RUN apt-get update && \
    apt-get install -y gcc && \
    rm -rf /var/lib/apt/lists/*

# set working directory
WORKDIR /app

# environment variable for output directory
RUN mkdir -p /app/output/

# copy and install Python dependencies
COPY requirements.txt .

RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt && \
    apt-get remove -y gcc && \
    apt-get autoremove -y

# copy main script
COPY query.py .
COPY wait-for-it.sh .
RUN chmod +x wait-for-it.sh

# run the script
CMD ["python", "query.py"]


