rm -rf build-target/log/*
./build-target/bin/stop-cluster.sh && ./build-target/bin/start-cluster.sh
./build-target/bin/flink run   -c com.example.dedup.DedupRefCountBenchmark   -p 1   ./flink-tests/flink-dedup-bench/target/flink-dedup-bench-1.16.3.jar