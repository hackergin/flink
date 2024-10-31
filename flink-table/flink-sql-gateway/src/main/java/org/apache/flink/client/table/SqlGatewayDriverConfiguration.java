package org.apache.flink.client.table;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

public class SqlGatewayDriverConfiguration {
    public static final ConfigOption<String> SQL_APPLICATION_JSON_PLAN =
            ConfigOptions.key("$internal.sql-gateway.driver.json-plan")
                    .stringType()
                    .noDefaultValue();
}
