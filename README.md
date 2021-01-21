# Heracles
This repository is based on Prometheus tsdb v0.8.0.

The design of the original Prometheus tsdb can be found [here](https://fabxc.org/blog/2017-04-10-writing-a-tsdb/).

The original compression methods are based on the Gorilla [white papers](http://www.vldb.org/pvldb/vol8/p1816-teller.pdf).

See also the [format documentation](docs/format/README.md).

## Benchamrks
### Setup
You can find the data set [here](https://mycuhk-my.sharepoint.com/:f:/g/personal/1155092207_link_cuhk_edu_hk/Ei2gQU_2J9ZJoULerBrXJjgBmBou4qNRg8HKxrCirQyYDg?e=FWEg3O).  
Please copy the files to folder __"testdata/bigdata/node_exporter"__.  

Prepare the dependency:
`$ go mod vendor`

### 1. Test insertion
Need to turn off profiling and set flag ne (use node exporter labels).  
`> cd cmd/tsdb`  
Random data  
`> go run -mod=vendor main.go bench write --osleep [baseline sleep seconds] --gsleep [group sleep seconds] --metrics [num of ts] --batch [batch size]`  
Timeseries data  
`> go run -mod=vendor main.go bench write --osleep [baseline sleep seconds] --gsleep [group sleep seconds] --metrics [num of ts] --batch [batch size] --timeseries`  

### 2. Test TSBS query benchmark
You can midify the `numSeries` in `db_bench_tsbs_test.go` to test different data volumes.  
Test Prometheus tsdb  
`> go test -mod=vendor -run ^$ -bench ^BenchmarkDBtsbs$ . -timeout 99999s -benchtime 10s -v`  
Test TGroup  
`> go test -mod=vendor -run ^$ -bench ^BenchmarkGroupDBtsbs$ . -timeout 99999s -benchtime 10s -v`