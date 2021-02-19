# Use python 3.6 
FROM python:3.6-slim

# setup the working directory
RUN mkdir /code && \
    cd /code

# Set the working directory to KPF-Pipeline
WORKDIR /code/pyNEID
ADD . /code/pyNEID

# Install the package
RUN pip3 install -r requirements.txt
