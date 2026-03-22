package org.example.demo;

import org.apache.spark.sql.SparkSession;

import java.io.Serializable;
import java.util.List;

public abstract class BroadcastUDFRegistry implements Serializable {
    protected abstract void initializeFromRows(String dataset, List<List<Object>> rows, String ddlOpt);

    protected abstract void initializeFromRows(String dataset, List<List<Object>> rows);

    protected abstract void updateBroadcast(SparkSession spark);

    protected abstract void registerUDFs(SparkSession spark);

    protected abstract void cleanup();
}
