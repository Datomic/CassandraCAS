// Copyright (c) Cognitect, Inc. All rights reserved.

package com.datomic;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.WriteTimeoutException;
import com.datastax.driver.core.policies.RetryPolicy;

import java.io.Closeable;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;

public class CassandraCAS implements Closeable {
    public static final String TABLESPACE = "datomic";
    public static final String id = "cas-test";
    public static final String SELECT = "select id, rev, map, val from datomic.datomic where id = ?";
    public static final String CREATE = "insert into datomic.datomic (id, rev, map, val) values (?, ?, ?, ?)";
    public static final String CAS = "update datomic.datomic set rev=?, map=?, val=? where id=? if rev=?";
    public static final ByteBuffer blob = ByteBuffer.wrap("test".getBytes());


    public static Cluster createCluster(String host, int port) {
        return Cluster.builder().addContactPoint(host)
                .withPort(port)
                .build();
    }

    public static long pid() {
        return Long.parseLong(ManagementFactory.getRuntimeMXBean().getName().replaceAll("@.*",""));
    }

    public static void print(Object o) {
        Object[] arr = (Object[]) o;
        System.out.print("[");
        for (int i = 0; i < arr.length; i++) {
            System.out.print(arr[i]);
            if (i + 1 < arr.length)
                System.out.print(", ");
        }
        System.out.println("]");
    }

    public static class ThreeRetryPolicy implements RetryPolicy {

        @Override
        public RetryDecision onReadTimeout(Statement statement, ConsistencyLevel cl, int i, int i1, boolean b, int retries) {
            return (retries == 3) ? RetryDecision.rethrow() : RetryDecision.retry(cl);
        }

        @Override
        public RetryDecision onWriteTimeout(Statement statement, ConsistencyLevel cl, WriteType writeType, int i, int i1, int retries) {
            System.out.println(retries);
            System.out.println(cl);
            return (retries == 3) ? RetryDecision.rethrow() : RetryDecision.retry(cl);
        }

        @Override
        public RetryDecision onUnavailable(Statement statement, ConsistencyLevel cl, int i, int i1, int retries) {
            return (retries == 3) ? RetryDecision.rethrow() : RetryDecision.retry(cl);
        }
    }
    public static final RetryPolicy retryPolicy = new ThreeRetryPolicy();

    public final Cluster cluster;
    public final Session session;
    public final PreparedStatement select;
    public final PreparedStatement create;
    public final PreparedStatement cas;

    @Override
    public void close() throws IOException {
        session.close();
        cluster.close();
    }

    public CassandraCAS(String host, int port) {
        cluster = createCluster(host, port);
        session = cluster.connect();
        create = session.prepare(CREATE);

        select = session.prepare(SELECT);
        select.setConsistencyLevel(ConsistencyLevel.QUORUM);
        select.setSerialConsistencyLevel(ConsistencyLevel.SERIAL);
        select.setRetryPolicy(retryPolicy);

        cas = session.prepare(CAS);
        cas.setConsistencyLevel(ConsistencyLevel.QUORUM);
        cas.setSerialConsistencyLevel(ConsistencyLevel.SERIAL);
        cas.setRetryPolicy(retryPolicy);
    }

    public boolean createRev() {
        BoundStatement bound = create.bind(new Object[]{id, 0L, "map", blob});
        ResultSet resultSet = session.execute(bound);
        return resultSet.isExhausted();
    }

    public long readRev() {
        BoundStatement bound = select.bind(new Object[]{id});
        ResultSet resultSet = session.execute(bound);
        return resultSet.one().getLong("rev");
    }

    public String casRev(long prev) {
        try {
            BoundStatement bound = cas.bind(new Object[]{(prev + 1), "map", blob, id, prev});
            Row row = session.execute(bound).one();
            if (row != null)
                return row.getBool(0) ? ":success" : ":fail";
            else
                return ":norow";
        } catch (WriteTimeoutException e) {
            return ":write-timeout";
        }
    }

    public void race(final int threads, final int pause, final long dest) {
        final BlockingQueue queue = new LinkedBlockingQueue();
        new Thread(new Runnable() {
            @Override
            public void run() {
                Object o = null;
                try {
                    while (true) {
                        o = queue.take();
                        if (o == queue) break;
                        print(o);
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();
        for (int i = 0; i < threads; i++) {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    long prev = readRev();
                    final long pid = pid();
                    final long tid = Thread.currentThread().getId();
                    try {
                        if (pause != 0) Thread.sleep(pause);
                        while (prev < dest) {
                            String result = casRev(prev);
                            queue.put(new Object[]{result, prev, prev + 1, tid, pid});
                            if (":success".equals(result)) {
                                prev = prev + 1;
                            } else {
                                prev = readRev();
                            }
                        }
                        queue.put(queue);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }).start();
        }
    }

    public static void main(String[] args) throws IOException {
        CassandraCAS driver = new CassandraCAS(args[0], Integer.parseInt(args[1]));
        if (args[2].equals("read"))
            System.out.println(driver.readRev());
        if (args[2].equals("create"))
            System.out.println(driver.createRev());
        if (args[2].equals("cas")) {
            long prev = driver.readRev();
            System.out.println("CAS to " + (prev + 1));
            System.out.println(driver.casRev(prev));
        }
        if (args[2].equals("race")) {
            driver.createRev();
            driver.race(Integer.parseInt(args[3]), Integer.parseInt(args[4]), Long.parseLong(args[5]));
        }
        driver.close();
    }
}
