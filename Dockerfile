FROM python:3.8
WORKDIR /usr/app/src
COPY requirements.txt ./requirements.txt
COPY runscript.sh ./runscript.sh
COPY . .
RUN mkdir -p dags/data/daily_updates
RUN pip install pandas
RUN pip install boto3
RUN pip install --user --upgrade pip
RUN pip install --no-cache-dir --user -r requirements.txt
CMD ["bash","./runscript.sh"]
