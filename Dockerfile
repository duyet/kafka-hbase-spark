FROM tiangolo/uwsgi-nginx-flask:flask

COPY . /app

# Get pip to download and install requirements:
RUN pip install -r /app/requirements.txt

# Expose ports
EXPOSE 8080

# Set the default directory where CMD will execute
WORKDIR /app
