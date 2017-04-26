FROM ches/kafka:0.10.2.0

MAINTAINER ContainerShip Developers <developers@containership.io>

# set user to root
USER root

# install dependencies
RUN apt-get update && apt-get install curl npm -y

# install node
RUN npm install -g n
RUN n 6.9.2

# create /app and add files
WORKDIR /app
ADD . /app

# install dependencies
RUN npm install

# Execute the run script
CMD node kafka.js
