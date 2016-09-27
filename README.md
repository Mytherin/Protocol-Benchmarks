This set of benchmarks measures the performance of database (and other systems') client protocols when transferring a large amount of data. The following systems are measured:

* PostgreSQL
* MariaDB (MySQL)
* DB2
* Oracle
* MonetDB
* MongoDB
* Hive

All the benchmarks in the paper are performed using the ODBC connector of the database (except for Hive, which uses the JDBC connector). However, there is also testing code for benchmarking the native client connector and the JDBC client connector of each of the databases.

# VM Image Download
All the benchmarks are performed on a VM running Ubuntu 16.04. The image of the VM can currently not be downloaded, however, in the future we will put up a link to download the VM [here](http://www.lipsum.com/). The experiments can be run by starting the VM and running the benchmark script using the command `python benchmark.py`. All the necessary database servers should be started automatically when the VM is launched. The results of the experiments will be put in the `/home/user/results` folder in CSV format. The graphs/tables can be generated using the command `R -f graph.R`.

# Additional Notes
For the MongoDB ODBC driver [this proprietary driver](http://www.simba.com/drivers/mongodb-odbc-jdbc/) is used. This driver requires a valid license file called `SimbaMongoDBODBCDriver.lic` to be in the users' home directory. This license file can be obtained by requesting a free trial of the software, after which they will email you a license file.
