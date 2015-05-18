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

## Running CAS Stress Test

The program

    java -jar target/CassandraCAS-0.0.jar {host} {port} raceSessions {nthreads} {msec} {target}

will start nthreads racing to CAS a counter from 0 up to target.  Each
thread will pause for msec between operations (or pass 0 msec for no pause).

## Running Multiple Threads on One Client Process

The following example is enough load to force some write retries on my
local dev box:

    java -jar target/CassandraCAS-0.0.jar 127.0.0.1 9042 create
    java -jar target/CassandraCAS-0.0.jar 127.0.0.1 9042 raceSessions 5 0 500

## Running Multiple Client Processes

Setup

    java -jar target/CassandraCAS-0.0.jar 127.0.0.1 9042 create

Once per client (need to start at about same time or first one can
finish before others begin)

    java -jar target/CassandraCAS-0.0.jar 127.0.0.1 9042 raceSessions 1 0 5000


## Validating CAS

* Collect the outputs from one or more clients running CAS stress test
  into a single file.
* Load the com.datomic.validate-cas namespace in a Clojure REPL.  No
  deps, so vanilla Clojure REPL will do.
* Run validate-file against the output file.  If the :valid? key is
  false, the validator thinks a consistency violation has occurred.

## Java Retry Bug

* When a write timeout occurs, the Java driver can be configured to call a `RetryPolicy` to allow clients flexibility in error handling.
* The `onWriteTimeout` method is passed a single `ConsistencyLevel` argument, described as "the original consistency level of the write that timed out."  This looks a bit sketchy since writes can set two kinds of consistency levels (CL and SCL), but the retry handler only gets informed about one.
* When a write timeout occurs on a `SCL=SERIAL` write, the consistency level passed to onWriteTimeout is `SERIAL`.  Retrying with the level given results in an `InvalidQueryException`.

In short, a query that is valid on initial try is deemed invalid on retry.  You can usually see this by running with the following settings

    java -Dcom.datomic.CassandraCASRetryPolicy=Documented -jar target/CassandraCAS-0.0.jar 127.0.0.1 9042 raceSessions 5 0 500

## Thread Safety Testing

Setup

    mvn clean package
    java -jar target/CassandraCAS-0.0.jar 127.0.0.1 9042 create

First use of driver is off main thread: not ok

    java -jar target/CassandraCAS-0.0.jar 127.0.0.1 9042 casThread

    com.datastax.driver.core.exceptions.NoHostAvailableException: All host(s) tried for query failed (no host was tried)
    	at com.datastax.driver.core.exceptions.NoHostAvailableException.copy(NoHostAvailableException.java:84)
    	at com.datastax.driver.core.DefaultResultSetFuture.extractCauseFromExecutionException(DefaultResultSetFuture.java:289)
    	at com.datastax.driver.core.DefaultResultSetFuture.getUninterruptibly(DefaultResultSetFuture.java:205)
    	at com.datastax.driver.core.AbstractSession.execute(AbstractSession.java:52)
    	at com.datomic.CassandraCAS.casRev(CassandraCAS.java:142)
    	at com.datomic.CassandraCAS$5.run(CassandraCAS.java:263)
    	at java.lang.Thread.run(Thread.java:745)
    Caused by: com.datastax.driver.core.exceptions.NoHostAvailableException: All host(s) tried for query failed (no host was tried)
    	at com.datastax.driver.core.RequestHandler.sendRequest(RequestHandler.java:107)
    	at com.datastax.driver.core.SessionManager.execute(SessionManager.java:538)
    	at com.datastax.driver.core.SessionManager.executeQuery(SessionManager.java:577)
    	at com.datastax.driver.core.SessionManager.executeAsync(SessionManager.java:119)

First use of driver is on main thread, then CAS from another thread: ok

    java -jar target/CassandraCAS-0.0.jar 127.0.0.1 9042 readMainThenCASThread

    CAS to 1

First use of driver is on main thread, then another thread touches
mbean then does CAS: not ok

    java -jar target/CassandraCAS-0.0.jar 127.0.0.1 9042 readMainThenMbeanCASThread

    CAS to 2
    com.datastax.driver.core.exceptions.NoHostAvailableException: All host(s) tried for query failed (no host was tried)
    	at com.datastax.driver.core.exceptions.NoHostAvailableException.copy(NoHostAvailableException.java:84)
    	at com.datastax.driver.core.DefaultResultSetFuture.extractCauseFromExecutionException(DefaultResultSetFuture.java:289)
    	at com.datastax.driver.core.DefaultResultSetFuture.getUninterruptibly(DefaultResultSetFuture.java:205)
    	at com.datastax.driver.core.AbstractSession.execute(AbstractSession.java:52)
    	at com.datomic.CassandraCAS.casRev(CassandraCAS.java:142)
    	at com.datomic.CassandraCAS$6.run(CassandraCAS.java:278)
    	at java.lang.Thread.run(Thread.java:745)
    Caused by: com.datastax.driver.core.exceptions.NoHostAvailableException: All host(s) tried for query failed (no host was tried)
    	at com.datastax.driver.core.RequestHandler.sendRequest(RequestHandler.java:107)
    	at com.datastax.driver.core.SessionManager.execute(SessionManager.java:538)
    	at com.datastax.driver.core.SessionManager.executeQuery(SessionManager.java:577)
    	at com.datastax.driver.core.SessionManager.executeAsync(SessionManager.java:119)
    	... 4 more


## Copyright

Copyright (c) Cognitect, Inc. All rights reserved.

