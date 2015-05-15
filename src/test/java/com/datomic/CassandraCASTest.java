package com.datomic;

import org.junit.Test;
import static org.junit.Assert.*;

import java.io.IOException;

/**
 * Created by stu on 5/15/15.
 */
public class CassandraCASTest {
    public static final int ONE_THREAD = 1;
    public static final int NO_PAUSE = 0;
    public static final int TARGET_REV = 10;

    @Test
    public void testMainThreadExecution() throws IOException {
        CassandraCAS driver = new CassandraCAS("127.0.0.1", 9042);
        try {
            driver.createRev();
            driver.relay(ONE_THREAD, NO_PAUSE, TARGET_REV);
            assertEquals(TARGET_REV,driver.readRev());
        } finally {
            driver.close();
        }
    }

    @Test
    public void testWorkerThreadExecution() throws IOException {
        CassandraCAS driver = new CassandraCAS("127.0.0.1", 9042);
        try {
            driver.createRev();
            driver.race(ONE_THREAD, NO_PAUSE, TARGET_REV);
            assertEquals(TARGET_REV,driver.readRev());
        } finally {
            driver.close();
        }
    }
}
