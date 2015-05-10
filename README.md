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

Start a 3+ node cassandra cluster.

## Running 

The program

    java com.datomic.CassandraCAS {host} {port} race {nthreads} {msec} {target}

will start nthreads racing to CAS a counter from 0 up to target.  Each
thread will pause for msec between operations (or pass 0 msec for no pause).

The following example is enough load to force some write retries on my
local dev box:

   java com.datomic.CassandraCAS 127.0.0.1 9042 race 5 0 500

## Copyright

Copyright (c) Cognitect, Inc. All rights reserved.

