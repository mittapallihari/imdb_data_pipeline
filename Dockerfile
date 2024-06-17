FROM apache/airflow:2.6.3

ADD README.md README.md
ADD requirements.txt .

RUN pip install -r requirements.txt
