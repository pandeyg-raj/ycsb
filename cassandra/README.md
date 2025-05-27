# Apache Cassandra 2.x CQL binding

## Creating a table for use with YCSB

For keyspace `ycsb`, table `usertable`:

    cqlsh> alter keyspace ycsb WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor': 5 } AND DURABLE_WRITES = true;
    cqlsh> USE ycsb;
    cqlsh> create table ycsb.usertable (
        y_id varchar primary key,
        field0 varchar,
        field1 varchar,
        field2 varchar,
        field3 varchar,
        field4 varchar,
        field5 varchar,
        field6 varchar,
        field7 varchar,
        field8 varchar,
        field9 varchar)
        WITH caching = { 'keys' : 'NONE', 'rows_per_partition' : '1'} AND compression = { 'enabled' : false };


bin/ycsb.sh load cassandra-cql -s -P electworkload -threads 8
bin/ycsb.sh run cassandra-cql -s -P electworkload -threads 8

mvn clean package -Psource-run


## Cassandra Configuration Parameters

- `hosts` (**required**)
  - Cassandra nodes to connect to.
  - No default.

* `port`
  * CQL port for communicating with Cassandra cluster.
  * Default is `9042`.

- `cassandra.keyspace`
  Keyspace name - must match the keyspace for the table created (see above).
  See http://docs.datastax.com/en/cql/3.1/cql/cql_reference/create_keyspace_r.html for details.

  - Default value is `ycsb`

- `cassandra.username`
- `cassandra.password`
  - Optional user name and password for authentication. See http://docs.datastax.com/en/cassandra/2.0/cassandra/security/security_config_native_authenticate_t.html for details.

* `cassandra.readconsistencylevel`
* `cassandra.writeconsistencylevel`

  * Default value is `QUORUM`
  - Consistency level for reads and writes, respectively. See the [DataStax documentation](http://docs.datastax.com/en/cassandra/2.0/cassandra/dml/dml_config_consistency_c.html) for details.

* `cassandra.maxconnections`
* `cassandra.coreconnections`
  * Defaults for max and core connections can be found here: https://datastax.github.io/java-driver/2.1.8/features/pooling/#pool-size. Cassandra 2.0.X falls under protocol V2, Cassandra 2.1+ falls under protocol V3.
* `cassandra.connecttimeoutmillis`
* `cassandra.useSSL`
  * Default value is false.
  - To connect with SSL set this value to true.
* `cassandra.readtimeoutmillis`
  * Defaults for connect and read timeouts can be found here: https://docs.datastax.com/en/drivers/java/2.0/com/datastax/driver/core/SocketOptions.html.
* `cassandra.tracing`
  * Default is false
  * https://docs.datastax.com/en/cql/3.3/cql/cql_reference/tracing_r.html
