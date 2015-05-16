## CassandraCAS

Compare-and-swap in Cassandra

## Setup

From Cassandra cqlsh:

    CREATE KEYSPACE IF NOT EXISTS datomic WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 3};
    CREATE TABLE IF NOT EXISTS datomic.datomic
    (
      id text PRIMARY KEY,
      rev bigint,
      map text,
      val blob
    );

Start a 3+ node cassandra cluster.  Get Cassandra running locally on 127.0.0.1, 2, 3.
[CCM](http://www.datastax.com/dev/blog/ccm-a-development-tool-for-creating-local-cassandra-clusters)
is great for this.

## Building

    mvn clean package

## Running 

The program

    java -jar target/CassandraCAS-0.0.jar {host} {port} raceSessions {nthreads} {msec} {target}

will start nthreads racing to CAS a counter from 0 up to target.  Each
thread will pause for msec between operations (or pass 0 msec for no pause).

The following example is enough load to force some write retries on my
local dev box:

    java -jar target/CassandraCAS-0.0.jar 127.0.0.1 9042 raceSessions 5 0 500

## Java Retry Bug

* When a write timeout occurs, the Java driver can be configured to call a `RetryPolicy` to allow clients flexibility in error handling.
* The `onWriteTimeout` method is passed a single `ConsistencyLevel` argument, described as "the original consistency level of the write that timed out."  This looks a bit sketchy since writes can set two kinds of consistency levels (CL and SCL), but the retry handler only gets informed about one.
* When a write timeout occurs on a `SCL=SERIAL` write, the consistency level passed to onWriteTimeout is `SERIAL`.  Retrying with the level given results in an `InvalidQueryException`.

In short, a query that is valid on initial try is deemed invalid on retry.  You can usually see this by running with the following settings

    java -Dcom.datomic.CassandraCASRetryPolicy=Documented -jar target/CassandraCAS-0.0.jar 127.0.0.1 9042 raceSessions 5 0 500

## Copyright

Copyright (c) Cognitect, Inc. All rights reserved.

