FROM python:2.7

#set directoty where CMD will execute
WORKDIR /usr/src/app
COPY requirements.txt ./
# Get pip to download and install requirements:
#add project files to the usr/src/app folder
RUN pip install --no-cache-dir -r requirements.txt
ADD . /usr/src/app
# COPY .env .env
# RUN export $(cat .env | xargs)
# ARG REDIS_HOST
# ARG REDIS_PORT
# ARG REDIS_PASSWORD
# ARG LOOPBACK_URL
# Expose ports
EXPOSE 8000
# default command to execute
CMD exec gunicorn NucleusOrderBook.wsgi:application --bind 0.0.0.0:8000 --workers 3