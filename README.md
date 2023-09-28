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

Install the Stackable Data Platform operators

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

    java -cp '/tmp/hbase-stackable/local/jars/hbase-gis-1.0.0.jar:/stackable/conf' tech.stackable.gis.hbase.Ingest wifi /tmp/wifi_4326.txt

Load bulk data

    java -cp '/tmp/hbase-stackable/local/jars/hbase-gis-1.0.0.jar:/stackable/conf' tech.stackable.gis.hbase.BulkIngest wifi a 100000 -75.99,-75.01,44.01,44.99

## Using the WithinFilter

    java -cp '/tmp/hbase-stackable/local/jars/hbase-gis-1.0.0.jar:/stackable/conf' \
    tech.stackable.gis.hbase.WithinQuery \
    remote \
    "POLYGON ((-73.980844 40.758703, \
        -73.987214 40.761369, \
        -73.990839 40.756400, \
        -73.984422 40.753642, \
        -73.980844 40.758703))"

You should get 26 results.

For the bulk-loaded data

    java -cp '/tmp/hbase-stackable/local/jars/hbase-gis-1.0.0.jar:/stackable/conf' \
    tech.stackable.gis.hbase.WithinQuery \
    remote \
    "POLYGON ((-76.0 44.97, \
        -76.0 45.0, \
        -75.9 45.0, \
        -75.9 44.97, \
        -76.0 44.97))"

You should a subset of the bulk-loaded back as the results of this query (due to the way that the query polygon and the limits of the randomly generated data have been defined.)

## Using the KNNFilter

    java -cp '/tmp/hbase-stackable/local/jars/hbase-gis-1.0.0.jar:/stackable/conf' tech.stackable.gis.hbase.KNNQuery -73.97000655 40.76098703 5

## Acknowledgements

Original code was written by Nick Dimiduk and Amandeep Khurana and is
available [here](https://github.com/hbaseinaction/gis)