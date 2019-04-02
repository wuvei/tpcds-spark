# tpcds-spark
TPC-DS queries in spark SQL

This project gains inspirations from [ssavvides/tpch-spark](https://github.com/ssavvides/tpch-spark) and the implementation is based on [maropu/spark-tpcds-datagen](https://github.com/maropu/spark-tpcds-datagen).



## Usage

### Setup
- Setup [Spark](https://spark.apache.org/) 2.4.0
- Setup SPARK_HOME:`export SPARK_HOME=<path_to_spark>`
- DownLoad tpcds-spark
```bash
git clone https://github.com/SimonZYC/tpcds-spark.git

cd /<path_to>/tpcds-spark/

sbt package
```

### Data generation

```bash
# We must execute programs in ubin directory
cd /<path_to>/tpcds-spark/ubin
./dsdgen.sh --output-location ../data

# For more options:
./dsdgen.sh --help
Usage: spark-submit --class <this class> --conf key=value <spark tpcds datagen jar> [Options]
Options:
  --output-location [STR]                Path to an output location
  --scale-factor [NUM]                   Scale factor (default: 1)
  --format [STR]                         Output format (default: parquet)
  --overwrite                            Whether it overwrites existing data (default: false)
  --partition-tables                     Whether it partitions output data (default: false)
  --use-double-for-decimal               Whether it prefers double types (default: false)
  --cluster-by-partition-columns         Whether it cluster output data by partition columns (default: false)
  --filter-out-null-partition-values     Whether it filters out NULL partitions (default: false)
  --table-filter [STR]                   Queries to filter, e.g., catalog_sales,store_sales
  --num-partitions [NUM]                 # of partitions (default: 100)
```

### Run All TPC-DS Queries

```bash
# We must execute programs in ubin directory
cd /<path_to>/tpcds-spark/ubin

./run-tpcs.sh --data-location [TPC-DS test data]

# We could add Spark arguments:
./run-tpcs.sh --data-location [TPC-DS test data] --master local[*]
```

### Run Specific TPC-DS Queries

```bash
# We must execute programs in ubin directory
cd /<path_to>/tpcds-spark/ubin

./run-tpcs.sh --data-location [TPC-DS test data] --query-filter "q2,q10"
```

Notice: There are queries which have the same names in V1.4 and V2.7. You could take a look at [reources](src/main/resources).

## Reference

[ssavvides/tpch-spark](https://github.com/ssavvides/tpch-spark)

[maropu/spark-tpcds-datagen](https://github.com/maropu/spark-tpcds-datagen)

[tpcds-kit](<https://github.com/databricks/tpcds-kit>)

[tpcds](http://www.tpc.org/tpcds/)