/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.planner.factories;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.streaming.api.functions.source.FromElementsFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class TestTimeTravelValuesTableFactory implements DynamicTableSourceFactory {

    public static final String IDENTIFIER = "test-time-travel";

    private static final ConfigOption<String> DATA_ID =
            ConfigOptions.key("data-id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The data id used to read the rows.");

    private static final AtomicInteger idCounter = new AtomicInteger(0);

    private static final Map<String, Collection<Tuple2<Long, List<RowData>>>> registeredRowData =
            new HashMap<>();

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        helper.validate();

        Optional<Long> snapshot = context.getCatalogTable().getSnapshot();

        String dataId =
                helper.getOptions().getOptional(DATA_ID).orElse(String.valueOf(idCounter.get()));
        if (snapshot.isPresent()) {
            return new TestTableSource(dataId, context.getObjectIdentifier(), snapshot.get());
        } else {
            return new TestTableSource(dataId, context.getObjectIdentifier());
        }
    }

    public static String registerRowData(Collection<Tuple2<Long, List<RowData>>> data) {
        String id = String.valueOf(idCounter.incrementAndGet());
        registeredRowData.put(id, data);
        return id;
    }

    @Override
    public String factoryIdentifier() {
        return "test-time-travel";
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return new HashSet<>();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(DATA_ID);
        return options;
    }

    private static class TestTableSource implements ScanTableSource {

        private @Nullable Long timestamp;

        private final String dataId;

        private final ObjectIdentifier objectIdentifier;

        public TestTableSource(String dataId, ObjectIdentifier objectIdentifier) {
            this.dataId = dataId;
            this.objectIdentifier = objectIdentifier;
        }

        public TestTableSource(String dataId, ObjectIdentifier objectIdentifier, Long timestamp) {
            this.dataId = dataId;
            this.objectIdentifier = objectIdentifier;
            this.timestamp = timestamp;
        }

        @Override
        public DynamicTableSource copy() {
            TestTableSource source = new TestTableSource(dataId, objectIdentifier, timestamp);
            return source;
        }

        @Override
        public String asSummaryString() {
            return "TestTableSource";
        }

        @Override
        public ChangelogMode getChangelogMode() {
            return ChangelogMode.insertOnly();
        }

        @Override
        public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
            return new SourceFunctionProvider() {
                @Override
                public SourceFunction<RowData> createSourceFunction() {
                    Collection<Tuple2<Long, List<RowData>>> rows = registeredRowData.get(dataId);

                    if (rows != null) {
                        List<RowData> rowDataList = new ArrayList<>();
                        if (timestamp != null) {
                            rowDataList.addAll(
                                    rows.stream()
                                            .filter(row -> row.f0 <= timestamp)
                                            .flatMap(row -> row.f1.stream())
                                            .collect(Collectors.toList()));
                        } else {
                            rowDataList.addAll(
                                    rows.stream()
                                            .flatMap(row -> row.f1.stream())
                                            .collect(Collectors.toList()));
                        }
                        return new FromElementsFunction<>(rowDataList);
                    } else {
                        return new FromElementsFunction<>();
                    }
                }

                @Override
                public boolean isBounded() {
                    return true;
                }
            };
        }
    }
}
