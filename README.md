docker-kafka
==============

##About

###Description
Docker image designed to run a kafka cluster on ContainerShip

###Author
ContainerShip Developers - developers@containership.io

##Usage
This image is designed to run kafka on a ContainerShip cluster. Running this image elsewhere is not recommended as the container may be unable to start.

###Configuration
This image will run as-is, with no additional environment variables set. For clustering to work properly, start the application in host networking mode. There are various optionally configurable environment variables:

* `ZOOKEEPER_APP_NAME` - Name of zookeeper application running on ContainerShip cluster (defaults to "zookeeper")
* `KAFKA_USE_PUBLIC_ADVERTISED_HOST` - If set to true or 1, brokers will register with public IPs for their advertised host (defaults to "false")

### Recommendations
* On your ContainerShip cluster, run this application using the `constraints.per_node=1` tag. Each node in your cluster will run an instance of kafka, creating a cluster of `n` hosts, where `n` is the number of follower hosts in your ContainerShip cluster.
* Start the application with `container_volume=/data` and `host_volume=/mnt/kafka` so data is persisted to the host system in case your container crashes. Additionally, by bind-mounting the volume, your data will be available for backup from ContainerShip Cloud.

##Contributing
Pull requests and issues are encouraged!
