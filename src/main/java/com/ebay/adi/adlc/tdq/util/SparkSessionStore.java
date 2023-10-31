package com.ebay.adi.adlc.tdq.util;

import org.apache.spark.sql.SparkSession;

public class SparkSessionStore {

    private static volatile SparkSessionStore instance;

    private SparkSessionStore() {
        store = new ThreadLocal<>();
    }

    public static SparkSessionStore getInstance() {
        if (instance == null) {
            synchronized (SparkSessionStore.class) {
                if (null == instance) {
                    instance = new SparkSessionStore();
                }
            }
        }
        return instance;
    }

    private ThreadLocal<SparkSession> store;

    public void storeSparkSession(SparkSession sparkSession) {
        store.set(sparkSession);
    }

    public SparkSession getSparkSession() {
        return store.get();
    }
}
