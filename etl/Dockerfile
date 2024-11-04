FROM apache/airflow:2.5.1-python3.9

# copy DAGs and requirements
COPY ETL_Pipeline.py /usr/local/airflow/dags/
COPY requirements.txt /requirements.txt

# install dependencies
RUN pip install -r /requirements.txt && \
    python -m nltk.downloader vader_lexicon stopwords && \
    python -m spacy download en_core_web_sm

# initialize Airflow database
RUN airflow db init

# run webserver and scheduler in foreground
CMD ["bash", "-c", "airflow webserver --port 8080 & airflow scheduler"]

