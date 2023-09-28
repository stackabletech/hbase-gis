# HBase GIS Utilities

A toolbox for working with very large Geographic Information System data.

## Requirements

* Maven and JDK 11 (or newer)
* [Protocol Buffers Compiler](https://grpc.io/docs/protoc-installation/)
* [Docker](https://docs.docker.com/engine/install/)
* A Kubernetes Cluster. For development and testing we recommend [Kind](https://kind.sigs.k8s.io/)
* [Stackable Command Line Utility](https://docs.stackable.tech/management/stable/stackablectl/)

## Building

    mvn clean package \
    && docker build -t docker.stackable.tech/hbase:2.4.12-gis .

## Deploy to Kind

Install the Stackable HBase Operator and dependencies

    kubectl create ns stackable-operators \
        && stackablectl -n stackable-operators op in commons secret zookeeper hdfs hbase

Load the custom HBase image into Kind

    kind load docker-image docker.stackable.tech/hbase:2.4.12-gis

Create an HBase cluster

    kubectl create ns gis && kubectl apply -n gis -f hbase.yaml

## Populate table

Open a shell to the HBase master Pod

    kubectl exec -it -n gis hbase-master-default-0 -- bash

Create a table

    echo "create 'wifi', 'a'" | /stackable/hbase/bin/hbase shell

Load data

    java -cp '/tmp/hbase-stackable/local/jars/hbaseia-gis-1.0.0.jar:/stackable/conf' tech.stackable.gis.hbase.Ingest wifi /tmp/wifi_4326.txt

## Using the WithinFilter

    java -cp '/tmp/hbase-stackable/local/jars/hbaseia-gis-1.0.0.jar:/stackable/conf' \
    tech.stackable.gis.hbase.WithinQuery \
    remote \
    "POLYGON ((-73.980844 40.758703, \
        -73.987214 40.761369, \
        -73.990839 40.756400, \
        -73.984422 40.753642, \
        -73.980844 40.758703))"

You should get 26 results.

## Using the KNNFilter

    java -cp '/tmp/hbase-stackable/local/jars/hbaseia-gis-1.0.0.jar:/stackable/conf' tech.stackable.gis.hbase.KNNQuery -73.97000655 40.76098703 5

## Acknowledgements

Original code was written by Nick Dimiduk and Amandeep Khurana and is
available [here](https://github.com/hbaseinaction/gis)