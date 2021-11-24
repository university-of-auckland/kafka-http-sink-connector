# https://strimzi.io/docs/operators/0.26.0/deploying.html#creating-new-image-from-base-str
FROM quay.io/strimzi/kafka:0.26.0-kafka-3.0.0
USER root:root
COPY ./target/*.jar /opt/kafka/plugins/
USER 1001
