./mvnw -U -pl flink-state-backends/flink-statebackend-rocksdb,flink-contrib/psl-utils -am -DskipTests clean package
cp flink-contrib/psl-utils/target/psl-utils-1.16.3.jar build-target/lib/
cp flink-state-backends/flink-statebackend-rocksdb/target/flink-statebackend-rocksdb-1.16.3.jar build-target/lib/