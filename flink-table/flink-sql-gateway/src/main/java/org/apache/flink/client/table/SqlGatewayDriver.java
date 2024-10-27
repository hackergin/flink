package org.apache.flink.client.table;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.PlanReference;
import org.apache.flink.table.api.TableEnvironment;

import static org.apache.flink.client.table.SqlGatewayDriverConfiguration.SQL_APPLICATION_JSON_PLAN;

public class SqlGatewayDriver {
    public static void main(String[] args) {
        TableEnvironment tableEnvironment = TableEnvironment.create(new Configuration());

        String jsonPlan =
                tableEnvironment.getConfig().getRootConfiguration().get(SQL_APPLICATION_JSON_PLAN);
        PlanReference planReference = PlanReference.fromJsonString(jsonPlan);
        tableEnvironment.executePlan(planReference);
    }
}
