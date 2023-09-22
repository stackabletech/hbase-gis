# Add server side gis filter to the HBase image
#
#   docker build -t docker.stackable.tech/hbase:2.4.12-gis .
#
FROM docker.stackable.tech/stackable/hbase:2.4.12-stackable0.0.0-dev

# TODO: figure out why HBASE_CLASSPATH or dynmic loading doesn't work
COPY  --chown=stackable:stackable target/hbaseia-gis-1.0.0.jar /tmp/hbase-stackable/local/jars/hbaseia-gis-1.0.0.jar
