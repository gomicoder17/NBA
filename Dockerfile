FROM apache/airflow:2.4.3

# Chrome
USER root

RUN apt-get update && apt-get install dpkg wget -y
RUN wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb
# COPY google-chrome-stable_current_amd64.deb .

RUN apt-get install -y fonts-liberation
RUN apt-get install -y libasound2
RUN apt-get install -y libatk-bridge2.0-0
RUN apt-get install -y libatk1.0-0
RUN apt-get install -y libatspi2.0-0
RUN apt-get install -y libcups2
RUN apt-get install -y libdbus-1-3
RUN apt-get install -y libdrm2
RUN apt-get install -y libgbm1
RUN apt-get install -y libgtk-3-0
RUN apt-get install -y libnspr4
RUN apt-get install -y libnss3
RUN apt-get install -y libxcomposite1
RUN apt-get install -y libxdamage1
RUN apt-get install -y libxfixes3
RUN apt-get install -y libxkbcommon0
RUN apt-get install -y libxrandr2
RUN apt-get install -y xdg-utils

RUN dpkg -i ./google-chrome-stable_current_amd64.deb

# Python
USER airflow

COPY requirements.txt .
COPY config.json .
COPY ./fonts/* ./fonts/

RUN pip install -r requirements.txt

