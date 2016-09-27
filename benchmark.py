import os
import time
import sys
import csv
import re
import json
import subprocess

# --------------------------- #
#           DATASETS          #
# --------------------------- #
datasets = ['lineitem', 'acs3yr', 'ontime']

# --------------------------- #
#    NETWORK CONFIGURATIONS   #
# --------------------------- #
# standard networks used for benchmarking
unlimited_network = {'name':'unlimited', 'throughput': -1, 'latency':-1}
lan_network = {'name':'gigabitlhd', 'throughput': 1000, 'latency':0.3}
wan_network = {'name':'100mbitlhd', 'throughput': 100, 'latency':25}


# networks used for latency/throughput isolations
latencies = [-1, 0.1, 1, 10, 100]
throughputs = [-1, 1000, 100, 10]
limited_networks = []

throughput = -1
for latency in latencies:
	limited_networks.append({'name': '%sms-%smb/s' % (latency if latency > 0 else 'unlimited', throughput if throughput > 0 else 'unlimited'), 'latency': latency, 'throughput': throughput})

latency = -1
for throughput in throughputs:
	limited_networks.append({'name': '%sms-%smb/s' % (latency if latency > 0 else 'unlimited', throughput if throughput > 0 else 'unlimited'), 'latency': latency, 'throughput': throughput})

throughput_limited_networks = []
for throughput in throughputs:
	throughput_limited_networks.append({'name': '%smb/s' % (throughput if throughput > 0 else 'unlimited'), 'latency': -1, 'throughput': throughput})


# --------------------------- #
#     SYSTEM CONFIGURATIONS   #
# --------------------------- #

# list of systems to benchmark
systems = []

# adds a system to the to list of systems to benchmark, output is written to 'filename'
def add_system(name, database, filename = None, tuples = None, dataset = 'lineitem', odbc_options = None, networks = None, extra_flags = None, minimalodbc = True):
	global systems
	system = dict()
	system['name'] = name
	system['db'] = database
	if networks == None:
		networks = [unlimited_network]
	networks = networks[:]
	if tuples == None:
		tuples = [1, 1000000]
	tuples = tuples[:]
	maxtuples = 10000000 if dataset == 'lineitem' else 1000000
	for i in range(len(tuples)):
		if tuples[i] > maxtuples:
			tuples[i] = maxtuples

	if odbc_options == None:
		odbc_options = ''

	if filename == None:
		filename = 'results.csv'

	system['odbc-options'] = odbc_options
	system['tuples'] = tuples
	system['networks'] = networks
	system['filename'] = filename
	system['dataset'] = dataset
	system['network'] = networks
	if extra_flags != None:
		for key,val in extra_flags.iteritems():
			system[key] = val
	if minimalodbc:
		system['minimalodbc'] = True
	systems.append(system)

default_run = False

# --------------------------- #
#   BENCHMARK CONFIGURATIONS  #
# --------------------------- #
# initial test for generating the first two graphs that shows the time split by stage and amount of bytes transferred for 1M lineitem entries
test_initial_lineitem_1m = default_run
initial_lineitem_1m_filename = 'lineitem1m.csv'

# perform test that isolates latency/throughput for each of the systems
test_throughput_latency = default_run
throughput_latency_filename = 'varyingnetworks.csv'

# perform the final evaluation of all systems on all three datasets
test_final_evaluation_all_datasets = True
final_evaluation_filename = 'finalalldatasets.csv'

# test for varying chunk size on different data sets
test_chunksize = default_run
chunksize_filename = 'chunksize.csv'
chunksizes =  [2000, 10000, 100000, 1000000, 10000000, 100000000]

# test for compression of binary row/column chunks
test_colrow_compression = default_run
colrow_compression_filename = 'colrowcompression.csv'

# test compression methods on all three datasets encoded in columnar binary chunks
test_compression = default_run
compression_filename = 'compression.csv'

# test different columnar compression methods
test_colcomp = default_run
colcomp_filename = 'colcomp.csv'
colcomps = ['none', 'pfor', 'binpack']

# test different serialization formats vs custom columnar serialization
test_serialization_formats = default_run
serialization_filename = 'serializations.csv'
serialization_formats = ['none', 'protobuf']

# test transfer time + size + compressibility of different string representations
test_string_representation = False
string_representation_filename = 'stringrepr.csv'

# benchmark ODBC vs JDBC vs Native clients of the different database systems
test_odbcjdbcnative = False
odbcjdbcnative_filename = 'odbcjdbcnative.csv'

# --------------------------- #
#         ODBC OPTIONS        #
# --------------------------- #
# MySQL ODBC Options, see: http://dev.mysql.com/doc/connector-odbc/en/connector-odbc-configuration-connection-parameters.html#codbc-dsn-option-flags
mysql_big_packet = 8
mysql_compressed_proto = 2048
mysql_no_cache = 1048576
mysql_forward_cursor = 2097152

MYSQL_COMPRESSED_ODBC_OPTIONS = "Option=%d;" % (mysql_big_packet + mysql_compressed_proto + mysql_no_cache + mysql_forward_cursor)
MYSQL_UNCOMPRESSED_ODBC_OPTIONS = "Option=%d;" % (mysql_big_packet + mysql_no_cache + mysql_forward_cursor)

# Postgres ODBC Options, see: https://odbc.postgresql.org/docs/config-opt.html
POSTGRESQL_ODBC_OPTIONS = "UniqueIndex=1;ReadOnly=1;Socket=1000000;Debug=0;UseDeclareFetch=1;Fetch=10000;"

# --------------------------- #
#    ADDITIONAL PARAMETERS    #
# --------------------------- #
result_path = '/home/user/results'

netcat_tempfile = '/tmp/netcat_file'

generate_graphs = False
generate_tables = False
table_filename = 'tables.tex'

nruns = 5
timeout = "30m"
netcat_port = 4444
fnull = open(os.devnull, 'w')

echo_results = True
extra_crash_dump = True

if test_initial_lineitem_1m:
	fname = initial_lineitem_1m_filename
	dataset = 'lineitem'
	tuples = [1000000]
	stages = ['connectonly', 'nofetch', 'noprint', 'print']
	networks = [unlimited_network]
	for stage in stages:
		add_system(name='monetdb-prot9-%s-%s' % (dataset, stage), database='monetdb', extra_flags = {'env': {'MONETDB_PROTOCOL': 'prot9'}, stage: True}, tuples = tuples, dataset = dataset, filename = fname, networks = networks)
		add_system(name='mariadb-compress-%s-%s' % (dataset, stage), database='mariadb', odbc_options = MYSQL_COMPRESSED_ODBC_OPTIONS, extra_flags = {stage: True}, tuples = tuples, dataset = dataset, filename = fname, networks = networks)
		add_system(name='mariadb-default-%s-%s' % (dataset, stage), database='mariadb', odbc_options = MYSQL_UNCOMPRESSED_ODBC_OPTIONS, extra_flags = {stage: True}, tuples = tuples, dataset = dataset, filename = fname, networks = networks)
		add_system(name='postgres-default-%s-%s' % (dataset, stage), database='postgres', odbc_options = POSTGRESQL_ODBC_OPTIONS, extra_flags = {stage: True}, tuples = tuples, dataset = dataset, filename = fname, networks = networks)
		add_system(name='oracle-default-%s-%s' % (dataset, stage), database='oracle', extra_flags = {stage: True}, tuples = tuples, dataset = dataset, filename = fname, networks = networks)
		add_system(name='db2-default-%s-%s' % (dataset, stage), database='db2', extra_flags = {stage: True}, tuples = tuples, dataset = dataset, filename = fname, networks = networks)
		add_system(name='hive-default-%s-%s' % (dataset, stage), database='hive', extra_flags = {stage: True}, tuples = tuples, dataset = dataset, filename = fname, networks = networks)
		add_system(name='mongodb-default-%s-%s' % (dataset, stage), database='mongodb', extra_flags = {stage: True}, tuples = tuples, dataset = dataset, filename = fname, networks = networks)
	add_system(name='netcat-csv-%s' % dataset, database='netcat', tuples = tuples, dataset = dataset, filename = fname, networks = networks, minimalodbc = False)

if test_throughput_latency:
	fname = throughput_latency_filename
	dataset = 'lineitem'
	tuples = [1, 1000000]
	networks = limited_networks
	add_system(name='monetdb-prot9-%s' % (dataset), database='monetdb', extra_flags = {'env': {'MONETDB_PROTOCOL': 'prot9'}}, tuples = tuples, dataset = dataset, filename = fname, networks = networks)
	add_system(name='mariadb-compress-%s' % (dataset,), database='mariadb', odbc_options = MYSQL_COMPRESSED_ODBC_OPTIONS, tuples = tuples, dataset = dataset, filename = fname, networks = networks)
	add_system(name='mariadb-default-%s' % (dataset,), database='mariadb', odbc_options = MYSQL_UNCOMPRESSED_ODBC_OPTIONS, tuples = tuples, dataset = dataset, filename = fname, networks = networks)
	add_system(name='postgres-default-%s' % (dataset,), database='postgres', odbc_options = POSTGRESQL_ODBC_OPTIONS, tuples = tuples, dataset = dataset, filename = fname, networks = networks)
	add_system(name='oracle-default-%s' % (dataset,), database='oracle', tuples = tuples, dataset = dataset, filename = fname, networks = networks)
	add_system(name='db2-default-%s' % (dataset,), database='db2', tuples = tuples, dataset = dataset, filename = fname, networks = networks)
	add_system(name='hive-default-%s' % (dataset,), database='hive', tuples = tuples, dataset = dataset, filename = fname, networks = networks)
	add_system(name='mongodb-default-%s' % (dataset,), database='mongodb', tuples = tuples, dataset = dataset, filename = fname, networks = networks)
	add_system(name='netcat-csv-%s' % dataset, database='netcat', tuples = tuples, dataset = dataset, filename = fname, networks = networks, minimalodbc = False)

if test_chunksize:
	fname = chunksize_filename
	tuples = [1, 10000000]
	networks = [unlimited_network]
	compression = 'snappy'
	for dataset in datasets:
		for chunksize in chunksizes:
			add_system(name='monetdb-prot10-none-%s-%d' % (dataset, chunksize), database='monetdb', extra_flags = {'compress': 'none', 'env': {'MONETDB_PROTOCOL': 'prot10', 'MONETDB_COLCOMP': 'none', 'MONETDB_BLOCKSIZE': str(chunksize)}}, tuples = tuples, dataset = dataset, filename = fname, networks = networks)
			add_system(name='monetdb-prot10-%s-%s-%d' % (compression, dataset, chunksize), database='monetdb', extra_flags = {'compress': compression, 'env': {'MONETDB_PROTOCOL': 'prot10compressed', 'MONETDB_COMPRESSION': compression, 'MONETDB_COLCOMP': 'none', 'MONETDB_BLOCKSIZE': str(chunksize)}}, tuples = tuples, dataset = dataset, filename = fname, networks = networks)

if test_colrow_compression:
	fname = colrow_compression_filename
	networks = [unlimited_network]
	compression_methods = ['snappy', 'lz4', 'gzip', 'xz', None]
	dataset = 'lineitem'
	tuples = [1000000]
	for compression_method in compression_methods:
		add_system(name='netcat-col-%s-%s' % (compression_method, dataset), database='netcat', extra_flags = {'netcat-file': os.path.join('/home/user/netcat', 'lineitem-1000000tpl-1000000chunksize.col'), 'compress': compression_method }, tuples = tuples, dataset = dataset, filename = fname, networks = networks, minimalodbc = False)
		add_system(name='netcat-row-%s-%s' % (compression_method, dataset), database='netcat', extra_flags = {'netcat-file': os.path.join('/home/user/netcat', 'lineitem-1000000tpl-1000000chunksize.row'), 'compress': compression_method }, tuples = tuples, dataset = dataset, filename = fname, networks = networks, minimalodbc = False)


if test_compression:
	fname = compression_filename
	networks = [unlimited_network]
	compression_methods = ['snappy', 'lz4', 'gzip', 'xz', None]
	networks = throughput_limited_networks
	tuples = [10000000]
	for compression_method in compression_methods:
		for dataset in datasets:
			add_system(name='netcat-%s-%s' % (compression_method, dataset), database='netcat', extra_flags = {'netcat-file': os.path.join('/home/user/netcat', '%s.proto' % dataset), 'compress': compression_method }, tuples = tuples, dataset = dataset, filename = fname, networks = networks, minimalodbc = False)

if test_colcomp:
	fname = colcomp_filename
	compression = 'snappy'
	tuples = [1,10000000]
	networks = throughput_limited_networks
	for dataset in datasets:
		for colcomp in colcomps:
			add_system(name='monetdb-prot10-none-%s-%s' % (colcomp,dataset,), database='monetdb', extra_flags = {'integer_only': True, 'compress': 'none', 'env': {'MONETDB_PROTOCOL': 'prot10', 'MONETDB_COLCOMP': colcomp}}, tuples = tuples, dataset = dataset, filename = fname, networks = networks)
			add_system(name='monetdb-prot10-%s-%s-%s' % (compression, colcomp,dataset,), database='monetdb', extra_flags = {'integer_only': True, 'compress': compression, 'env': {'MONETDB_PROTOCOL': 'prot10compressed', 'MONETDB_COMPRESSION': compression, 'MONETDB_COLCOMP': colcomp}}, tuples = tuples, dataset = dataset, filename = fname, networks = networks)


if test_serialization_formats:
	fname = serialization_filename
	compression = 'snappy'
	tuples = [1,10000000]
	networks = throughput_limited_networks
	for dataset in datasets:
		for serialization in serialization_formats:
			add_system(name='monetdb-prot10-none-%s-%s' % (serialization,dataset,), database='monetdb', extra_flags = {'integer_only': True, 'compress': 'none', 'env': {'MONETDB_PROTOCOL': 'prot10', 'MONETDB_COLCOMP': serialization}}, tuples = tuples, dataset = dataset, filename = fname, networks = networks)
			add_system(name='monetdb-prot10-%s-%s-%s' % (compression, serialization,dataset,), database='monetdb', extra_flags = {'integer_only': True, 'compress': compression, 'env': {'MONETDB_PROTOCOL': 'prot10compressed', 'MONETDB_COMPRESSION': compression, 'MONETDB_COLCOMP': serialization}}, tuples = tuples, dataset = dataset, filename = fname, networks = networks)

if test_string_representation:
	fname = string_representation_filename
	compression = 'snappy'
	tuples = [1,10000000]
	print("todo: this requires recompilation currently")
	exit(1)


if test_odbcjdbcnative:
	fname = odbcjdbcnative_filename
	tuples = [1,10000000]
	dataset = 'lineitem'
	networks = [unlimited_network]
	add_system(name='monetdb-prot9-%s-%s' % (dataset, stage), database='monetdb', extra_flags = {'env': {'MONETDB_PROTOCOL': 'prot9'}, stage: True}, tuples = tuples, dataset = dataset, filename = fname, networks = networks, minimalodbc = False)
	add_system(name='mariadb-compress-%s-%s' % (dataset, stage), database='mariadb', odbc_options = MYSQL_COMPRESSED_ODBC_OPTIONS, extra_flags = {stage: True}, tuples = tuples, dataset = dataset, filename = fname, networks = networks, minimalodbc = False)
	add_system(name='mariadb-default-%s-%s' % (dataset, stage), database='mariadb', odbc_options = MYSQL_UNCOMPRESSED_ODBC_OPTIONS, extra_flags = {stage: True}, tuples = tuples, dataset = dataset, filename = fname, networks = networks, minimalodbc = False)
	add_system(name='postgres-default-%s-%s' % (dataset, stage), database='postgres', odbc_options = POSTGRESQL_ODBC_OPTIONS, extra_flags = {stage: True}, tuples = tuples, dataset = dataset, filename = fname, networks = networks, minimalodbc = False)
	add_system(name='oracle-default-%s-%s' % (dataset, stage), database='oracle', extra_flags = {stage: True}, tuples = tuples, dataset = dataset, filename = fname, networks = networks, minimalodbc = False)
	add_system(name='db2-default-%s-%s' % (dataset, stage), database='db2', extra_flags = {stage: True}, tuples = tuples, dataset = dataset, filename = fname, networks = networks, minimalodbc = False)
	add_system(name='hive-default-%s-%s' % (dataset, stage), database='hive', extra_flags = {stage: True}, tuples = tuples, dataset = dataset, filename = fname, networks = networks, minimalodbc = False)
	add_system(name='mongodb-default-%s-%s' % (dataset, stage), database='mongodb', extra_flags = {stage: True}, tuples = tuples, dataset = dataset, filename = fname, networks = networks, minimalodbc = False)


if test_final_evaluation_all_datasets:
	fname = final_evaluation_filename
	networks = [unlimited_network, lan_network, wan_network]
	tuples = [1, 10000000]
	for dataset in datasets:
		add_system(name='monetdb-prot10-%s' % dataset, database='monetdb', extra_flags = {'env': {'MONETDB_PROTOCOL': 'prot10', 'MONETDB_COLCOMP': 'none'}}, tuples = tuples, dataset = dataset, filename = fname, networks = networks)
		add_system(name='monetdb-prot10-snappy-%s' % dataset, database='monetdb', extra_flags = {'env': {'MONETDB_PROTOCOL': 'prot10compressed', 'MONETDB_COMPRESSION': 'snappy', 'MONETDB_COLCOMP': 'pfor'}}, tuples = tuples, dataset = dataset, filename = fname, networks = networks)
		#add_system(name='monetdb-prot9-%s' % dataset, database='monetdb', extra_flags = {'env': {'MONETDB_PROTOCOL': 'prot9'}}, tuples = tuples, dataset = dataset, filename = fname, networks = networks)
		add_system(name='mariadb-compress-%s' % dataset, database='mariadb', odbc_options = MYSQL_COMPRESSED_ODBC_OPTIONS, tuples = tuples, dataset = dataset, filename = fname, networks = networks)
		add_system(name='mariadb-default-%s' % dataset, database='mariadb', odbc_options = MYSQL_UNCOMPRESSED_ODBC_OPTIONS, tuples = tuples, dataset = dataset, filename = fname, networks = networks)
		add_system(name='postgres-default-%s' % dataset, database='postgres', odbc_options = POSTGRESQL_ODBC_OPTIONS, tuples = tuples, dataset = dataset, filename = fname, networks = networks)
		add_system(name='oracle-default-%s' % dataset, database='oracle', tuples = tuples, dataset = dataset, filename = fname, networks = networks)
		add_system(name='db2-default-%s' % dataset, database='db2', tuples = tuples, dataset = dataset, filename = fname, networks = networks)
		add_system(name='hive-default-%s' % dataset, database='hive', tuples = tuples, dataset = dataset, filename = fname, networks = networks)
		add_system(name='mongodb-default-%s' % dataset, database='mongodb', tuples = tuples, dataset = dataset, filename = fname, networks = networks)
		add_system(name='netcat-csv-%s' % dataset, database='netcat', tuples = tuples, dataset = dataset, filename = fname, networks = networks, minimalodbc = False)


def netcat_listener(compression):
	nclistener = subprocess.Popen(['nc', '-l', '-p', '%d' % netcat_port], stdout=subprocess.PIPE if compression != None else fnull, stderr=fnull)
	if compression == None:
		return nclistener
	else:
		if compression == 'lz4':
			uncompress_cmd = 'lz4 -dc'
		elif compression == 'lz4-heavy':
			uncompress_cmd = 'lz4 -dc -9'
		elif compression == 'gzip':
			uncompress_cmd = 'gunzip -f'
		elif compression == 'xz':
			uncompress_cmd = 'xz -d'
		elif compression == 'snappy':
			uncompress_cmd = 'snzip -d'
		return subprocess.Popen(uncompress_cmd.split(' '), stdin=nclistener.stdout, stdout = fnull)

def syscall(cmd):
	return os.system(cmd)

def rxbytes():
	return int(re.search("RX bytes:(\d+)", os.popen("ifconfig lo").read()).groups(0)[0])

def rxpackets():
	return int(re.search("RX packets:(\d+)", os.popen("ifconfig lo").read()).groups(0)[0])

def benchmark_command(cmd, system, protocol, network, tuple, r, dummy, csvwriter):
	timeoutcmd = 'timeout --foreground -s KILL ' +  timeout
	if system['db'] == 'netcat':
		listener = netcat_listener(system['compress'] if 'compress' in system else None)
		timeoutcmd = ''
	startbytes = rxbytes()
	startpackets = rxpackets()
	start = time.time() # 
	retcode = syscall('/usr/bin/time --format=\'{"io_page_faults": %F, "memory_max_kb": %M, "cpu_kernel_sec": %S, "cpu_user_sec": %U}\' --quiet --output=timing ' + timeoutcmd + ' ' + cmd)
	duration = time.time() - start
	transmittedbytes = rxbytes() - startbytes
	transmittedpackets = rxpackets() - startpackets
	if retcode != 0:
		duration = -1
		if extra_crash_dump and dummy:
			sys.stdout.write('Crashing command: %s' % cmd)
	stats = {'system': system['name'], 'db': system['db'], 'protocol': protocol, 'network': network['name'], 'throughput': network['throughput'], 'latency': network['latency'], 'tuple': tuple, 'run': r, 'time': duration, "bytes" : transmittedbytes, 'packets': transmittedpackets, 'timeout' : int(retcode != 0), 'bin_orientation' : system['fileext'] if 'fileext' in system else '', 'bin_chunksize': system['chunksize'] if 'chunksize' in system else '', 'bin_compress':system['compress'] if 'compress' in system else '', 'dataset': system['dataset'] if 'dataset' in system else ''}
	try:
		stats.update(json.load(open('timing')))
	except:
		stats['timeout'] = -1
		pass
	if not dummy:
		csvwriter.writerow(stats)
		if echo_results:
			sys.stdout.write('Time: %f s, Transfer: %f MB' % (duration, transmittedbytes / 1024.0 / 1024.0))
	os.remove('timing')

	if system['db'] == 'netcat':
		listener.kill()


oq = """set colsep '|'
set ARRAYSIZE 100
SET LINESIZE 132
SET PAGESIZE 6000
set echo off
set feedback off
set linesize 1000
set pagesize 0
set sqlprompt ''
set trimspool on
set headsep off
SELECT * FROM lineitem where rownum < &1;
quit
"""

oqfile = open("query-oracle.sql", "w")
oqfile.write(oq)
oqfile.close()


def result_file(fname):
	return os.path.join(result_path, fname)

def open_result_file(fname):
	write_header = not os.path.isfile(result_file(fname))
	# new file, write header
	f = open(result_file(fname), 'a+')
	w = csv.DictWriter(f, ['system', 'db', 'protocol', 'network', 'throughput', 'latency', 'tuple', 'run', 'timeout', 'time', 'bytes', 'packets', 'cpu_kernel_sec', 'cpu_user_sec',  'io_page_faults', 'memory_max_kb', 'bin_orientation', 'bin_chunksize', 'bin_compress', 'dataset'])
	if write_header:
		w.writeheader()
	return (w, f)


def pretty_print(text, maxlength = 60, header = True, footer = True):
	if header:
		sys.stdout.write('-' * maxlength + '\n')
	leftpad = (maxlength - 2 - len(text)) / 2
	rightpad = maxlength - 2 - len(text) - leftpad
	sys.stdout.write('|' + ' ' * leftpad + text + ' ' * rightpad + '|\n')
	if footer:
		sys.stdout.write('-' * maxlength + '\n')

# initialization
for system in systems:
	if os.path.isfile(result_file(system['filename'])):
		os.remove(result_file(system['filename']))

systemindex = 0
for system in systems:
	systemindex += 1
	if systemindex > 1: sys.stdout.write('\n')
	(csvwriter, csvfile) = open_result_file(system['filename'])
	networks = system['network'] if 'network' in system else [unlimited_network]
	pretty_print(system['name'], footer = False)
	index = 0
	for network in networks:
		tuples = system['tuples']
		for tuple in tuples:
			index += 1
			pretty_print('Network: %s, Tuples: %d' % (network['name'], tuple), header = index > 1)
			for r in range(nruns):
				sys.stdout.write("\t%d/%d\t" % (r + 1, nruns))
				syscall("sudo tc qdisc del dev lo root netem 2>/dev/null")
				if network['throughput'] > 0 or network['latency'] > 0:
					syscall("sudo tc qdisc add dev lo root netem %s %s" % ('delay %fms' % network['latency'] if network['latency'] > 0 else '', 'rate %dmbit' % network['throughput'] if network['throughput'] > 0 else ''))

				query = "SELECT * FROM lineitem LIMIT %d" % tuple
				if 'dataset' in system:
					query = "SELECT * FROM %s LIMIT %d" % (system['dataset'], tuple)
				querycmd = ""
				jdbcflags = ''
				odbcdriver = ''
				odbccmd = None
				odbccmd_minimal = None
				if system['db'] == 'postgres':
					querycmd = 'psql %s --host 127.0.0.1 -w -t -A -c "%s" > /dev/null' % ('--set=sslcompression=1 --set=sslmode=require --set=keepalives=0' if 'compress' in system else '', query)
					jdbcflags = 'org.postgresql.Driver jdbc:postgresql://127.0.0.1/user user user'
					odbccmd = 'isql PostgreSQL -d, < query > /dev/null'
					odbccmd = '/home/user/pmodbc PostgreSQL "%s" 0 > /dev/null' % query
					odbccmd_minimal = '/home/user/pmodbc PostgreSQL "%s" 0 "%s"' % (query, system['odbc-options'] if 'odbc-options' in system else '')
				elif system['db'] == 'mariadb':
					querycmd = 'mysql %s --unbuffered=true --host=127.0.0.1 user --skip-column-names --batch -e "%s" > /dev/null'  % ('--compress' if 'compress' in system else '', query)
					jdbcflags = 'org.mariadb.jdbc.Driver jdbc:mysql://127.0.0.1/user user null'
					odbccmd = 'isql MySQL -d, < query > /dev/null'
					odbccmd = '/home/user/pmodbc MySQL "%s" 0 %s > /dev/null' % (query, system['odbc-options'] if 'odbc-options' in system else '')
					odbccmd_minimal = '/home/user/pmodbc MySQL "%s" 0 "%s"' % (query, system['odbc-options'] if 'odbc-options' in system else '')
				elif system['db'] == 'monetdb':
					if 'params' in system:
						querycmd = '/home/user/monetdb-install/bin/mclient -h 127.0.0.1 -p 50001 -fcsv -s "%s" %s > /dev/null' % (query, system['params'])
						jdbcflags = None
						odbccmd = None
						odbcdriver = None
					else:
						if 'integer_only' in system:
							if 'dataset' not in system:
								query = "SELECT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_shipdate, l_commitdate, l_receiptdate FROM lineitem LIMIT %d" % tuple
							elif system['dataset'] == 'acs3yr':
								query = 'SELECT sporder, puma, st, pwgtp, agep, cit, citwp, cow, ddrs, dear, deye, dout, dphy, drat, dratx, drem, eng, fer, gcl, gcm, gcr, hins1, hins2, hins3, hins4, hins5, hins6, hins7, intp, jwmnp, jwrip, jwtr, lanx, mar, marhd, marhm, marht, marhw, marhyp, mig, mil, mlpa, mlpb, mlpc, mlpd, mlpe, mlpf, mlpg, mlph, mlpi, mlpj, mlpk, nwab, nwav, nwla, nwlk, nwre, oip, pap, relp, retp, sch, schg, schl, semp, sex, ssip, ssp, wagp, wkhp, wkl, wkw, yoep, anc, anc1p, anc2p, decade, dis, drivesp, esp, esr, hicov, hisp, indp, jwap, jwdp, lanp, migpuma, migsp, msp, nativity, nop, oc, paoc, pernp, pincp, pobp, povpip, powpuma, powsp, privcov, pubcov, qtrbir, rac1p, rac2p, rac3p, racaian, racasn, racblk, racnhpi, racnum, racsor, racwht, rc, sfn, sfr, vps, waob, fagep, fancp, fcitp, fcitwp, fcowp, fddrsp, fdearp, fdeyep, fdoutp, fdphyp, fdratp, fdratxp, fdremp, fengp, fesrp, fferp, fgclp, fgcmp, fgcrp, fhins1p, fhins2p, fhins3c, fhins3p, fhins4c, fhins4p, fhins5c, fhins5p, fhins6p, fhins7p, fhisp, findp, fintp, fjwdp, fjwmnp, fjwrip, fjwtrp, flanp, flanxp, fmarhdp, fmarhmp, fmarhtp, fmarhwp, fmarhyp, fmarp, fmigp, fmigsp, fmilpp, fmilsp, foccp, foip, fpap, fpobp, fpowsp, fracp, frelp, fretp, fschgp, fschlp, fschp, fsemp, fsexp, fssip, fssp, fwagp, fwkhp, fwklp, fwkwp, fyoep, pwgtp1, pwgtp2, pwgtp3, pwgtp4, pwgtp5, pwgtp6, pwgtp7, pwgtp8, pwgtp9, pwgtp10, pwgtp11, pwgtp12, pwgtp13, pwgtp14, pwgtp15, pwgtp16, pwgtp17, pwgtp18, pwgtp19, pwgtp20, pwgtp21, pwgtp22, pwgtp23, pwgtp24, pwgtp25, pwgtp26, pwgtp27, pwgtp28, pwgtp29, pwgtp30, pwgtp31, pwgtp32, pwgtp33, pwgtp34, pwgtp35, pwgtp36, pwgtp37, pwgtp38, pwgtp39, pwgtp40, pwgtp41, pwgtp42, pwgtp43, pwgtp44, pwgtp45, pwgtp46, pwgtp47, pwgtp48, pwgtp49, pwgtp50, pwgtp51, pwgtp52, pwgtp53, pwgtp54, pwgtp55, pwgtp56, pwgtp57, pwgtp58, pwgtp59, pwgtp60, pwgtp61, pwgtp62, pwgtp63, pwgtp64, pwgtp65, pwgtp66, pwgtp67, pwgtp68, pwgtp69, pwgtp70, pwgtp71, pwgtp72, pwgtp73, pwgtp74, pwgtp75, pwgtp76, pwgtp77, pwgtp78, pwgtp79, pwgtp80 FROM acs3yr LIMIT %d' % tuple
							elif system["dataset"] == "ontime":
								query = 'SELECT "FlightDate", "OriginAirportID", "OriginAirportSeqID", "OriginCityMarketID", "DestAirportID", "DestAirportSeqID", "DestCityMarketID", "Div1AiportID", "Div1AirportSeqID", "Div2AirportID", "Div2AirportSeqID", "Div3AirportID", "Div3AirportSeqID", "Div4AirportID", "Div4AirportSeqID", "Div5AirportID", "Div5AirportSeqID" FROM "ontime" LIMIT %d' % tuple
						if 'strings_type' in system:
							query = "SELECT l_returnflag,l_linestatus,l_shipinstruct,l_shipmode,l_comment FROM lineitem_decimal_string LIMIT %d" % tuple
						if 'strings_query' in system:
							query = "SELECT l_returnflag,l_linestatus,l_shipinstruct,l_shipmode,l_comment FROM lineitem LIMIT %d" % tuple
						if 'tablename' in system:
							query = "SELECT l_comment FROM %s LIMIT %d" % (system['tablename'], tuple)
						if 'columnname' in system:
							query = "SELECT %s FROM lineitem LIMIT %d" % (system['columnname'], tuple)
						querycmd = 'mclient -h 127.0.0.1 -p 50001 -fcsv -s "%s" > /dev/null' % query
						jdbcflags = 'nl.cwi.monetdb.jdbc.MonetDriver jdbc:monetdb://127.0.0.1:50001/database monetdb monetdb'
						os.environ['MONETDB_PROTOCOL'] = 'prot9'
						odbccmd = 'isql MonetDB -d, < query > /dev/null'
						odbccmd = '/home/user/pmodbc MonetDB "%s" 0 > /dev/null' % query
						odbccmd_minimal = '/home/user/pmodbc MonetDB "%s" 0 ""' % query.replace('"', '\\\"')
				elif system['db'] == 'db2':
					query = query.replace("ontime", '"ontime"')
					db2qfile = open("db2query", "w")
					db2qfile.write("connect to remotedb user user using user; \n" + query + ";\n")
					db2qfile.close()
					querycmd = 'db2 -tf db2query > /dev/null;'
					jdbcflags = 'com.ibm.db2.jcc.DB2Driver jdbc:db2://127.0.0.1:50000/db user user'
					os.environ['DB2INSTANCE'] = 'user'
					odbccmd = 'isql DB2_SAMPLE -d, user user < query > /dev/null'
					odbccmd = '/home/user/pmodbc DB2_SAMPLE "%s" 0 > /dev/null' % query
					odbccmd_minimal = '/home/user/pmodbc DB2_SAMPLE "%s" 0 ""' % query
				elif system['db'] == 'oracle':
					# for JDBC/ODBCV
					query = "SELECT * FROM %s where rownum < %d" % (system['dataset'] if 'dataset' in system else 'lineitem', tuple)
					if 'connectonly' in system:
						query = ''
					os.environ['TNS_ADMIN'] = '/home/user/oracleconfig'
					querycmd = 'sqlplus system/oracle@//127.0.0.1:49161/XE @query-oracle.sql %d > /dev/null' % tuple
					jdbcflags = 'oracle.jdbc.driver.OracleDriver jdbc:oracle:thin:@127.0.0.1:49161:XE system oracle'
					odbccmd = 'isql Oracle -d, < query > /dev/null'
					odbccmd = '/home/user/pmodbc Oracle "%s" 0 > /dev/null' % query
					odbccmd_minimal = '/home/user/pmodbc Oracle "%s" 0 ""' % query
				elif system['db'] == 'mongodb':
					querycmd = 'mongoexport -d lineitem  -c things --csv --fields "l_orderkey,l_partkey,l_suppkey,l_linenumber,l_quantity,l_extendedprice,l_discount,l_tax,l_returnflag,l_linestatus,l_shipdate,l_commitdate,l_receiptdate,l_shipinstruct,l_shipmode,l_comment" --limit %d > /dev/null 2> /dev/null' % tuple
					jdbcflags = None
					odbccmd = None
					odbcdriver = None
					odbccmd_minimal = '/home/user/pmodbc MongoDB64 "%s" 0 ""' % query.replace('"', '\\\"').replace("lineitem", "lineitem.things").replace('acs3yr', 'acs3yr.things').replace('ontime', 'ontime.things')
				elif system['db'] == 'hbase':
					os.environ['HBASE_HEAPSIZE'] = '10g'
					query = "scan 'lineitem',{LIMIT=>%d}" % tuple
					querycmd = 'hbase shell < query > /dev/null 2> /dev/null'
					jdbcflags = None
					odbccmd = None
					odbcdriver = None
				elif system['db'] == 'hive':
					querycmd = None
					jdbcflags = 'org.apache.hive.jdbc.HiveDriver jdbc:hive2://localhost:10000 user null'
					odbccmd = None
					odbcdriver = None
					odbccmd_minimal = '/home/user/pmodbc Hive64 "%s" 0 ""' % query
				elif system['db'] == 'netcat':
					if 'netcat-file' in system:
						filename = system['netcat-file']
					elif 'chunksize' in system:
						filename = '/home/user/netcat/lineitem-%dtpl-%dchunksize.%s' % (tuple, system['chunksize'], system['fileext'])
					elif 'dataset' in system:
						syscall('head -n %d %s > %s' % (tuple, os.path.join('/home/user/netcat', '%s.csv' % dataset), netcat_tempfile))
						filename = netcat_tempfile
					compress_cmd = ''
					if 'compress' in system:
						if system['compress'] == 'lz4': 
							compress_cmd = 'lz4 -c - |'
						elif system['compress'] == 'lz4-heavy': 
							compress_cmd = 'lz4 -9 -c - |'
						elif system['compress'] == 'gzip': 
							compress_cmd = 'gzip |'
						elif system['compress'] == 'xz': 
							compress_cmd = 'xz -z |'
						elif system['compress'] == 'snappy': 
							compress_cmd = 'snzip -c |'
					querycmd = 'cat %s | %s nc 127.0.0.1 %d' % (filename, compress_cmd, netcat_port)
					jdbcflags = None
					odbccmd = None
					odbcdriver = None
				else:
					exit("unknown db %s" % system['db'])

				qfile = open("query", "w")
				qfile.write(query)
				qfile.write("\n")
				qfile.close()

				jdbccmd = 'java -Xmx10G -Djava.security.egd=file:/dev/./urandom -cp /home/user/java/pmjc.jar:/home/user/java/db2jcc4.jar:/home/user/java/monetdb-jdbc-2.23.jar:/home/user/java/mariadb-java-client-1.4.6.jar:/home/user/java/ojdbc6_g.jar:/home/user/java/postgresql-9.4.1209.jar:/home/user/java/hive-jdbc-2.1.0-standalone.jar:/home/user/java/hadoop-common-2.6.4.jar nl.cwi.da.pmjc.Pmjc %s "%s" 1000' % (jdbcflags, query)
				
				if 'minimalodbc' in system:
					if 'env' in system:
						for key,val in system['env'].iteritems():
							os.environ[key] = val

					# special case for hive
					if system['db'] == 'hive':
						if 'nofetch' in system:
							jdbccmd += " false false true"
						elif 'connectonly' in system:
							jdbccmd += " false true false"
						elif 'print' in system:
							jdbccmd += " true false false"
						else:
							jdbccmd += " false false false"
						odbccmd_minimal = jdbccmd + " >/dev/null 2>/dev/null"
					else:
						if 'nofetch' in system:
							odbccmd_minimal += " 0 1"
						if 'connectonly' in system:
							odbccmd_minimal += " 0 0"
						if 'print' in system:
							odbccmd_minimal = odbccmd_minimal.replace(" 0 ", " 1 ") + '>/dev/null'
					if r == 0:
						# get caches hot before first run
						benchmark_command(odbccmd_minimal, system, 'odbc-noprint', network, tuple, r, True, csvwriter)
					# odbc (pmodbc)
					benchmark_command(odbccmd_minimal, system, 'odbc-noprint', network, tuple, r, False, csvwriter)

					if 'env' in system:
						for key,val in system['env'].iteritems():
							os.environ[key] = ''
				else:
					jdbccmd += " false false false"
					# special case for hive
					if querycmd is None:
						querycmd = jdbccmd
						jdbccmd = None
						jdbcflags = None

					if 'connectonly' in system:
						jbccmd = None
						jdbcflags = None
						odbccmd = None

					# getting caches hot
					benchmark_command(querycmd, system, 'native', network, tuple, r, True, csvwriter)

					# native client
					benchmark_command(querycmd, system, 'native', network, tuple, r, False, csvwriter)
					if 'compress' not in system:
						# odbc (isql)
						if odbccmd is not None:
							benchmark_command(odbccmd, system, 'odbc', network, tuple, r, False, csvwriter)
						# jdbc
						if jdbcflags is not None:
							benchmark_command(jdbccmd, system, 'jdbc', network, tuple, r, False, csvwriter)
				csvfile.flush()
				sys.stdout.write('\n')
				os.remove('query')

			syscall("sudo tc qdisc del dev lo root netem 2>/dev/null")
			time.sleep(0.1)
exit()
