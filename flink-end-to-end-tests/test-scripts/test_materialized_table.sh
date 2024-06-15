#!/usr/bin/env bash
################################################################################
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

KAFKA_VERSION="3.2.3"
CONFLUENT_VERSION="7.2.2"
CONFLUENT_MAJOR_VERSION="7.2"
# Check the Confluent Platform <> Apache Kafka compatibility matrix when updating KAFKA_VERSION
KAFKA_SQL_VERSION="universal"
SQL_JARS_DIR=${END_TO_END_DIR}/flink-sql-gateway-test/target/dependencies
TEST_FILE_SYSTEM_JAR=$(find "$SQL_JARS_DIR" | grep "filesystem-test" )
KAFKA_JAR=$(find "$SQL_JARS_DIR" | grep "kafka" )

cp $TEST_FILE_SYSTEM_JAR ${FLINK_DIR}/lib/
cp $KAFKA_JAR ${FLINK_DIR}/lib/

CURRENT_DIR=`cd "$(dirname "$0")" && pwd -P`
source "${CURRENT_DIR}"/common.sh
source "${CURRENT_DIR}"/kafka-common.sh \
  ${KAFKA_VERSION} \
  ${CONFLUENT_VERSION} \
  ${CONFLUENT_MAJOR_VERSION} \
  ${KAFKA_SQL_VERSION}

MATERIALIZED_TABLE_DATA_DIR="${TEST_DATA_DIR}/materialized_table"
mkdir -p ${MATERIALIZED_TABLE_DATA_DIR}
SQL_GATEWAY_REST_PORT=8083

start_cluster
start_taskmanagers 3

# Set address for sql gateway.
set_config_key "sql-gateway.endpoint.rest.address" "localhost"
set_config_key "sql-gateway.endpoint.rest.port" "8083"
export FLINK_CONF_DIR="${FLINK_DIR}/conf"
start_sql_gateway

# prepare Kafka
echo "Preparing Kafka..."

setup_kafka_dist
start_kafka_cluster

function open_session() {
  local session_options=$1

  if [ -z "$session_options" ]; then
    session_options="{}"
  fi

  curl -s -X POST \
    -H "Content-Type: application/json" \
    -d "{\"properties\": $session_options}" \
    "http://localhost:$SQL_GATEWAY_REST_PORT/sessions" | jq -r '.sessionHandle'
}

function configure_session() {
  local session_handle=$1
  local statement=$2
  
  response=$(curl -s -X POST \
    -H "Content-Type: application/json" \
    -d "{\"statement\": \"$statement\"}" \
    http://localhost:$SQL_GATEWAY_REST_PORT/sessions/$session_handle/configure-session)
  
  if [ "$response" != "{}" ]; then
    echo "Configure session $session_handle $statement failed: $response"
    exit 1
  fi
  echo "Configured session $session_handle $statement successfully"
}

function close_session() {
  local session_handle=$1
  curl -s -X POST \
    -H "Content-Type: application/json" \
    -d "{\"session_handle\": \"$session_handle\"}" \
    http://localhost:$SQL_GATEWAY_REST_PORT/sessions/close
}

function execute_statement() {
  local session_handle=$1
  local statement=$2
  
  local response=$(curl -s -X POST \
    -H "Content-Type: application/json" \
    -d "{\"statement\": \"$statement\"}" \
    http://localhost:$SQL_GATEWAY_REST_PORT/v3/sessions/$session_handle/statements)

  local operation_handle=$(echo $response | jq -r '.operationHandle')
  if [ -z "$operation_handle" ] || [ "$operation_handle" == "null" ]; then
    echo "Failed to execute statement: $statement, response: $response"
    exit 1
  fi
  get_operation_result $session_handle $operation_handle
}

function get_operation_result() {
  local session_handle=$1
  local operation_handle=$2

  local fields_array=()
  local next_uri="v3/sessions/$session_handle/operations/$operation_handle/result/0"
  while [ ! -z "$next_uri" ] && [ "$next_uri" != "null" ];
  do
    response=$(curl -s -X GET \
      -H "Content-Type:  \
       application/json" \
      http://localhost:$SQL_GATEWAY_REST_PORT/$next_uri)
    result_type=$(echo $response | jq -r '.resultType')
    result_kind=$(echo $response | jq -r '.resultKind')
    next_uri=$(echo $response | jq -r '.nextResultUri')
    errors=$(echo $response | jq -r '.errors')
    if [ "$errors" != "null" ]; then
      echo "fetch operation $operation_handle failed: $errors"
      exit 1
    fi
    if [ result_kind == "SUCCESS" ]; then
      fields_array+="SUCCESS"
      break;
    fi
    if [ "$result_type" != "NOT_READY" ] && [ "$result_kind" == "SUCCESS_WITH_CONTENT" ]; then
      new_fields=$(echo $response | jq -r '.results.data[].fields')
      fields_array+=$new_fields
    else
      sleep 1
    fi
  done
  echo $fields_array
}

function create_kafka_source() {
  local session_handle=$1
  local topic=$2
  local table_name=$3
  
  local operation_handle=$(curl -s -X POST \
    -H "Content-Type: application/json" \
    -d "{\"statement\": \"CREATE TABLE $table_name (\
      \`timestamp\` TIMESTAMP_LTZ(3),\
      \`user\` STRING,\
      \`type\` STRING\
    ) WITH (\
      'connector' = 'kafka',\
      'topic' = '$topic',\
      'properties.bootstrap.servers' = 'localhost:9092',\
      'scan.startup.mode' = 'earliest-offset',\
      'format' = 'json',\
      'json.timestamp-format.standard' = 'ISO-8601'\
    )\"}" \
    http://localhost:$SQL_GATEWAY_REST_PORT/sessions/$session_handle/statements/ | jq -r '.operationHandle')

    get_operation_result $session_handle $operation_handle
}

function create_materialized_table_in_continous_mode() {
  local session_handle=$1
  local table_name=$2

  local operation_handle=$(curl -s -X POST \
    -H "Content-Type: application/json" \
    -d "{\"statement\": \"CREATE MATERIALIZED TABLE $table_name \
        PARTITIONED BY (ds) \
        with (\
          'format' = 'json',\
          'sink.rolling-policy.rollover-interval' = '10s',\
          'sink.rolling-policy.check-interval' = '10s'\
        )\
        FRESHNESS = INTERVAL '10' SECOND \
        AS SELECT \
          DATE_FORMAT(\`timestamp\`, 'yyyy-MM-dd') AS ds, \
          user, \
          type \
        FROM kafka_source\"}" \
    http://localhost:$SQL_GATEWAY_REST_PORT/sessions/$session_handle/statements | jq -r '.operationHandle')

  get_operation_result $session_handle $operation_handle
}

function create_filesystem_source() {
  local session_handle=$1
  local table_name=$2
  local path=$3
  
  local operation_handle=$(curl -s -X POST \
    -H "Content-Type: application/json" \
    -d "{\"statement\": \"CREATE TABLE $table_name (\
      \`timestamp\` TIMESTAMP_LTZ(3),\
      \`user\` STRING,\
      \`type\` STRING\
    ) WITH (\
      'connector' = 'filesystem',\
      'path' = '$path',\
      'format' = 'csv'\
    )\"}" \
    http://localhost:$SQL_GATEWAY_REST_PORT/sessions/$session_handle/statements/ | jq -r '.operationHandle')
}

function create_materialized_table_in_full_mode() {
  local session_handle=$1
  local table_name=$2

  local operation_handle=$(curl -s -X POST \
    -H "Content-Type: application/json" \
    -d "{\"statement\": \"CREATE MATERIALIZED TABLE $table_name\
        PARTITIONED BY (ds)\
        with (\
          'format' = 'json'\
        )\
        FRESHNESS = INTERVAL '10' SECOND\
        REFRESH_MODE = FULL\
        AS SELECT\
          ds,\
          user,\
          count(type) as type_cnt\
          FROM (\
            SELECT\
              DATE_FORMAT(\`timestamp\`, 'yyyy-MM-dd') AS ds,\
              user,\
              type\
              FROM filesystem_source\
          ) GROUP BY\
                 ds,\
                 user\"}" \
    http://localhost:$SQL_GATEWAY_REST_PORT/sessions/$session_handle/statements | jq -r '.operationHandle')

  get_operation_result $session_handle $operation_handle
}

############################## init session enviroment #############################
# 1. create a session with catalog store options and workflow scheduler options
# 2. create the test catalog
# 3. create a kafka source for test materialized table in continuous mode
# 4. create a filesystem source for test materialized table in full mode
####################################################################################

# init a session with catalog store options
session_options="{\"table.catalog-store.kind\": \"file\", \"table.catalog-store.file.path\": \"file:///$MATERIALIZED_TABLE_DATA_DIR/catalog-store\", \"workflow-scheduler.type\": \"embedded\"}"
session_handle=$(open_session "$session_options")

# configure_session "$session_handle" "add jar 'file://$TEST_FILE_SYSTEM_JAR'"
# configure_session "$session_handle" "add jar 'file://$KAFKA_JAR'"
configure_session "$session_handle" "create database test_db"
mkdir -p $MATERIALIZED_TABLE_DATA_DIR/catalog
configure_session "$session_handle" "create catalog test_catalog with (\
  'type' = 'test-filesystem',\
  'default-database' = 'test_db',\
  'path' = 'file:///$MATERIALIZED_TABLE_DATA_DIR/catalog'\
)"

mkdir -p $MATERIALIZED_TABLE_DATA_DIR/catalog/test_db

configure_session "$session_handle" "use catalog test_catalog"

# create kafka source table
create_kafka_source "$session_handle" "test-kafka-topic" "kafka_source"

# create filesystem source table
mkdir -p $TEST_DATA_DIR/filesystem_source
create_filesystem_source $session_handle "filesystem_source" $TEST_DATA_DIR/filesystem_source/

############################# test materialized table in continuous mode #############################

# create materialized table in continous mode
create_materialized_table_in_continous_mode "$session_handle" "my_materialized_table_in_continuous_mode"

# send message to kafka source table
echo "sending message to kafka source table"
send_messages_to_kafka '{"timestamp": "2024-06-17T08:00:00Z", "user": "Alice", "type": "WARNING"}' "test-kafka-topic"
send_messages_to_kafka '{"timestamp": "2024-06-17T08:00:00Z", "user": "Bob", "type": "INFO"}' "test-kafka-topic"


# verify the data of materialized table in continuous mode
configure_session "$session_handle" "set 'execution.runtime-mode'='batch'"

function verify_materialized_table_in_continuous_mode() {
  local expected_data=$@

  local query_result=$(execute_statement "$session_handle" "select * from my_materialized_table_in_continuous_mode order by \`ds\`, \`user\`, \`type\`")

  if [ "$query_result" = "$expected_data" ]; then
    echo "verify materialized table in continuous mode success"
    return 0
  else
    echo "verify materialized table in continuous mode failed"
    echo "expected data: $expected_data"
    echo "actual data: $query_result"
    return 1
  fi
}

echo "Verifying the data of materialized table in continuous mode..."
if ! retry_times 20 5 'verify_materialized_table_in_continuous_mode [ "2024-06-17", "Alice", "WARNING" ] [ "2024-06-17", "Bob", "INFO" ]'; then
  echo "verify materialized table in continuous mode failed after 20 retries"
  exit 1
fi

# suspend the materialized table
mkdir -p $MATERIALIZED_TABLE_DATA_DIR/savepoint
configure_session "$session_handle" "set 'execution.checkpointing.savepoint-dir'='file://$MATERIALIZED_TABLE_DATA_DIR/savepoint'"
execute_statement "$session_handle" "alter materialized table my_materialized_table_in_continuous_mode suspend"

# # check savepoint exists
# if [ ! -d "$MATERIALIZED_TABLE_DATA_DIR/savepoint/savepoint-*" ]; then
#   echo "ERROR: Can't find savepoint file"
#   exit 1
# fi

# resume the materialized table
execute_statement "$session_handle" "alter materialized table my_materialized_table_in_continuous_mode resume"

# send message to kafka
send_messages_to_kafka '{"timestamp": "2024-06-17T08:00:00Z", "user": "Charlie", "type": "ERROR"}' "test-kafka-topic"

# verify the data of materialized table
echo "Verifying the data of materialized table after resuming..."

if ! retry_times 20 5 'verify_materialized_table_in_continuous_mode
  [ "2024-06-17", "Alice", "WARNING" ] [ "2024-06-17", "Bob", "INFO" ] [ "2024-06-17", "Charlie", "ERROR" ]'; then
  echo "Failed to verify the data of materialized table after resuming."
  exit 1
fi

# drop materialized table
execute_statement "$session_handle" "DROP MATERIALIZED TABLE IF EXISTS my_materialized_table_in_continuous_mode"


############################# test materialized table in full mode #############################

# insert data into filesystem source
cat >> $TEST_DATA_DIR/filesystem_source/data.csv << EOF
2024-06-17 00:00:00Z,Alice,WARING
2024-06-17 00:00:01Z,Bob,ERROR
EOF

# create materialized table in full mode
create_materialized_table_in_full_mode $session_handle "my_materialized_table_in_full_mode"

# verify the data of materialized table
function verify_materialized_table_in_full_mode() {
  local expected_data=$@

  local query_result=$(execute_statement "$session_handle" "select * from my_materialized_table_in_full_mode order by \`ds\`, \`user\`")

  if [ "$query_result" = "$expected_data" ]; then
    echo "verify materialized table in continuous full success"
    return 0
  else
    echo "verify materialized table in continuous mode failed"
    echo "expected data: $expected_data"
    echo "actual data: $query_result"
    return 1
  fi
}

echo "Verifying materialized table in full mode..."
if ! retry_times 30 10 'verify_materialized_table_in_full_mode [ "2024-06-17", "Alice", 1 ] [ "2024-06-17", "Bob", 1 ]'; then
  echo "Verify materialized table in full mode failed after 30 retries"
  exit 1
fi


# suspend the materialized table
echo "Suspending materialized table..."
execute_statement $session_handle "ALTER MATERIALIZED TABLE my_materialized_table_in_full_mode SUSPEND"

# resume the materialized table
echo "Resuming materialized table..."
execute_statement $session_handle "ALTER MATERIALIZED TABLE my_materialized_table_in_full_mode RESUME"

# insert more data into filesystem source
cat >> $TEST_DATA_DIR/filesystem_source/data.csv << EOF
2024-06-17 00:00:00Z,Alice,ERROR
2024-06-17 00:00:00Z,Charlie,ERROR
EOF

# verify the data of materialized table
echo "Verifying materialized table after resuming..."

if ! retry_times 30 10 'verify_materialized_table_in_full_mode [ "2024-06-17", "Alice", 2 ] [ "2024-06-17", "Bob", 1 ] [ "2024-06-17", "Charlie", 1 ]'; then
  echo "Failed to verify materialized table after resuming."
  exit 1
fi


# stop cluster
stop_sql_gateway
stop_cluster
stop_kafka_cluster