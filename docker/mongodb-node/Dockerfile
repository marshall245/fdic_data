# Set base image
FROM ubuntu:latest

# Set /bin/bash as entry point
ENTRYPOINT ['/bin/bash', '-c']

# Get mongodb
RUN apt update

# Import MongoDB public GPG key AND create a MongoDB list file
RUN apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv EA312927

RUN apt install -y --no-install-recommends software-properties-common

RUN echo "deb http://repo.mongodb.org/apt/ubuntu $(cat /etc/lsb-release | grep DISTRIB_CODENAME | cut -d= -f2)/mongodb-org/3.2 multiverse" | tee /etc/apt/sources.list.d/mongodb-org-3.2.list

# Update apt-get sources AND install MongoDB
RUN apt update && apt-get install -y mongodb-org

# Create a data directory
RUN mkdir -p /data/db

# Expose the standard mongodb port
EXPOSE 27017

ENTRYPOINT ["/usr/bin/mongod", "--bind_ip=0.0.0.0"]

#CMD ["echo $PATH && mongod --bind_ip '0.0.0.0'"]
