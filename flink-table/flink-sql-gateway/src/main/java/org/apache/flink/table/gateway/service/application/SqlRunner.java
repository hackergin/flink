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

package org.apache.flink.table.gateway.service.application;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.internal.TableEnvironmentInternal;
import org.apache.flink.table.gateway.api.endpoint.EndpointVersion;
import org.apache.flink.table.gateway.api.operation.OperationHandle;
import org.apache.flink.table.gateway.api.session.SessionEnvironment;
import org.apache.flink.table.gateway.api.session.SessionHandle;
import org.apache.flink.table.gateway.environment.SqlGatewayStreamExecutionEnvironment;
import org.apache.flink.table.gateway.service.context.DefaultContext;
import org.apache.flink.table.gateway.service.context.SessionContext;
import org.apache.flink.table.gateway.service.operation.OperationExecutor;
import org.apache.flink.table.gateway.service.operation.OperationManager;
import org.apache.flink.table.gateway.service.result.ResultFetcher;
import org.apache.flink.table.operations.CallProcedureOperation;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.resource.ResourceManager;
import org.apache.flink.util.MutableURLClassLoader;
import org.apache.flink.util.concurrent.Executors;

import java.net.URLClassLoader;
import java.util.Collections;
import java.util.Iterator;

/** Runner to run the statements in application mode. */
public class SqlRunner {

    public static void run(String script) {
        DefaultContext defaultContext =
                DefaultContext.load(
                        (Configuration)
                                StreamExecutionEnvironment.getExecutionEnvironment(
                                                new Configuration())
                                        .getConfiguration(),
                        Collections.emptyList(),
                        false);
        EmbeddedSessionContext sessionContext =
                EmbeddedSessionContext.create(
                        defaultContext,
                        SessionHandle.create(),
                        SessionEnvironment.newBuilder().build());
        Iterator<Operation> operations = SyntaxParser.of(sessionContext).parse(script);
        while (operations.hasNext()) {
            Operation operation = operations.next();
            EmbeddedOperationExecutor executor =
                    (EmbeddedOperationExecutor)
                            sessionContext.createOperationExecutor(new Configuration());
            print(executor.executeOperation(executor.getTableEnvironment(), operation));
        }
    }

    static void print(ResultFetcher fetcher) {
        // TODO: print results into the log.
    }

    private static class EmbeddedSessionContext extends SessionContext {

        private EmbeddedSessionContext(
                DefaultContext defaultContext,
                SessionHandle sessionId,
                EndpointVersion endpointVersion,
                Configuration sessionConf,
                URLClassLoader classLoader,
                SessionState sessionState,
                OperationManager operationManager) {
            super(
                    defaultContext,
                    sessionId,
                    endpointVersion,
                    sessionConf,
                    classLoader,
                    sessionState,
                    operationManager);
        }

        public static EmbeddedSessionContext create(
                DefaultContext defaultContext,
                SessionHandle sessionId,
                SessionEnvironment environment) {
            Configuration sessionConfig = new Configuration();
            return new EmbeddedSessionContext(
                    defaultContext,
                    sessionId,
                    environment.getSessionEndpointVersion(),
                    sessionConfig,
                    (MutableURLClassLoader) Thread.currentThread().getContextClassLoader(),
                    initializeSessionState(
                            environment,
                            sessionConfig,
                            new ResourceManager(
                                    sessionConfig,
                                    (MutableURLClassLoader)
                                            Thread.currentThread().getContextClassLoader()),
                            sessionId),
                    new OperationManager(Executors.newDirectExecutorService()));
        }

        @Override
        public OperationExecutor createOperationExecutor(Configuration executionConfig) {
            return new EmbeddedOperationExecutor(this, executionConfig);
        }
    }

    private static class EmbeddedOperationExecutor extends OperationExecutor {

        public EmbeddedOperationExecutor(SessionContext context, Configuration executionConfig) {
            super(context, executionConfig);
        }

        public ResultFetcher executeOperation(TableEnvironmentInternal tableEnv, Operation op) {
            OperationHandle handle = OperationHandle.create();
            if (op instanceof CallProcedureOperation) {
                // if the operation is CallProcedureOperation, we need to set the stream environment
                // context to it since the procedure will use the stream environment
                try {
                    SqlGatewayStreamExecutionEnvironment.setAsContext(
                            sessionContext.getUserClassloader());
                    // TODO: get statement from parser
                    return executeOperation(tableEnv, handle, op, null, null);
                } finally {
                    SqlGatewayStreamExecutionEnvironment.unsetAsContext();
                }
            } else {
                return sessionContext.isStatementSetState()
                        // TODO: get statement from parser
                        ? executeOperationInStatementSetState(tableEnv, handle, op)
                        : executeOperation(tableEnv, handle, op, null, null);
            }
        }
    }
}
