package org.apache.flink.table.planner.runtime.batch.sql;

import org.apache.flink.configuration.BatchExecutionOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.planner.factories.TestUpdateDeleteTableFactory;

import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class TimeTravelITCase {
    private StreamExecutionEnvironment env;
    private StreamTableEnvironment tEnv;

    @Before
    public void setup() {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        tEnv = StreamTableEnvironment.create(env, EnvironmentSettings.inBatchMode());
        tEnv.getConfig().set(BatchExecutionOptions.ADAPTIVE_AUTO_PARALLELISM_ENABLED, false);
    }

    @Test
    public void testTimeTravel() {
        String dataId = registerData();
        tEnv.executeSql(
                String.format(
                        "CREATE TABLE t ("
                                + " a int PRIMARY KEY NOT ENFORCED,"
                                + " b string,"
                                + " c double) WITH"
                                + " ('connector' = 'test-update-delete', "
                                + "'data-id' = '%s')",
                        dataId));

        TableResult tableResult = tEnv.executeSql("SELECT * FROM t FOR SYSTEM_TIME AS OF TIMESTAMP '2023-04-05 10:00:00'");
//        TableResult tableResult = tEnv.executeSql("SELECT * FROM t FOR SYSTEM_TIME AS OF TO_TIMESTAMP_LTZ(100, 3)");

        tableResult.collect();

    }

    private String registerData() {
        List<RowData> values = createValue();
        return TestUpdateDeleteTableFactory.registerRowData(values);
    }

    private List<RowData> createValue() {
        List<RowData> values = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            values.add(GenericRowData.of(i, StringData.fromString("b_" + i), i * 2.0));
        }
        return values;
    }
}
