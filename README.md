This set of benchmarks measures the performance of database (and other systems') client protocols when transferring a large amount of data. The following systems are measured:

* PostgreSQL
* MariaDB (MySQL)
* DB2
* Oracle
* MonetDB
* MongoDB
* Hive

All the benchmarks are performed using the ODBC connector of the database (except for Hive, which uses the JDBC connector). However, there is also testing code for benchmarking the native client connector and the JDBC client connector of each of the databases.

In addition to the current systems, we measure the implementation of a new protocol in PostgreSQL and MonetDB. The implementation of these can be found here:

* [PostgreSQL Source Code](https://github.com/Mytherin/postgres)
* [MonetDB Source Code](http://dev.monetdb.org/hg/MonetDB/file/a7ebdda88223)

# Paper
Mark Raasveldt and Hannes Mühleisen: _Don't Hold My Data Hostage - A Case For Client Protocol Redesign_, 43rd International Conference on Very Large Data Bases (VLDB2017)
[PDF](http://www.vldb.org/pvldb/vol10/p1022-muehleisen.pdf)

# VM Image Download
All the benchmarks are performed on a VM running Ubuntu 16.04. The image of the VM can be downloaded [here](https://zenodo.org/record/1305845). The login credentials to the VM are username: user, password: user. The experiments can be run by starting the VM and running the benchmark script using the command `python benchmark.py`. The results of the experiments will be put in the `/home/user/results` folder in CSV format. The graphs/tables can be generated using the command `R -f graph.R`.

The different database systems can be started as follows:

##### PostgreSQL
```bash
sudo service postgresql start
```

##### MySQL
```bash
sudo service mysql start
```

##### DB2
```bash
db2start
```

##### Oracle
```bash
docker run -d -p 49160:22 -p 49161:1521 -v /home/user:/opt/user wnameless/oracle-xe-11g
```

##### Hive
```bash
$HIVE_HOME/bin/hive --service hiveserver2
```

##### MonetDB
```bash
/home/user/monetdb-install/bin/mserver5 --set gdk_nr_threads=1 --set mapi_port=50001 --dbpath=/home/user/monetdb/database --set varchar_maximum_fixed=3 --set optimizer_pipeline=sequential
```

##### PostgreSQL++
```bash
export PGDATA=/home/user/Sources/postgres/pgdata
/home/user/Sources/postgres/build/bin/postgres -p 5678
```

##### PostgreSQL++C
```bash
export PGDATA=/home/user/Sources/postgres/pgdata_compress
POSTGRES_COMPRESSION=true /home/user/Sources/postgres/build/bin/postgres -p 5700
```

# Additional Notes
For the MongoDB ODBC driver [this proprietary driver](http://www.simba.com/drivers/mongodb-odbc-jdbc/) is used. This driver requires a valid license file called `SimbaMongoDBODBCDriver.lic` to be in the users' home directory. This license file can be obtained by requesting a free trial of the software, after which they will email you a license file. In case this driver is no longer available, we have [archived it as well](https://zenodo.org/record/1307303).
