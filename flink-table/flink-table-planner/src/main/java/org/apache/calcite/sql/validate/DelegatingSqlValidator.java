/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.calcite.sql.validate;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.runtime.CalciteException;
import org.apache.calcite.runtime.Resources;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlDelete;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlMatchRecognize;
import org.apache.calcite.sql.SqlMerge;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlUpdate;
import org.apache.calcite.sql.SqlWindow;
import org.apache.calcite.sql.SqlWith;
import org.apache.calcite.sql.SqlWithItem;
import org.apache.calcite.sql.validate.implicit.TypeCoercion;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;

public class DelegatingSqlValidator extends SqlValidatorImpl {

    private SqlValidator parent;

    public DelegatingSqlValidator(SqlValidatorImpl parent) {
        super(
                parent.getOperatorTable(),
                parent.getCatalogReader(),
                parent.getTypeFactory(),
                parent.config());
        this.parent = parent;
    }

    @Override
    public SqlValidatorCatalogReader getCatalogReader() {
        return parent.getCatalogReader();
    }

    @Override
    public SqlOperatorTable getOperatorTable() {
        return parent.getOperatorTable();
    }

    @Override
    public SqlNode validate(SqlNode topNode) {
        return parent.validate(topNode);
    }

    @Override
    public SqlNode validateParameterizedExpression(
            SqlNode topNode, Map<String, RelDataType> nameToTypeMap) {
        return parent.validateParameterizedExpression(topNode, nameToTypeMap);
    }

    @Override
    public void validateQuery(
            SqlNode node, @Nullable SqlValidatorScope scope, RelDataType targetRowType) {
        parent.validateQuery(node, scope, targetRowType);
    }

    @Override
    public RelDataType getValidatedNodeType(SqlNode node) {
        return parent.getValidatedNodeType(node);
    }

    @Override
    public @Nullable RelDataType getValidatedNodeTypeIfKnown(SqlNode node) {
        return parent.getValidatedNodeTypeIfKnown(node);
    }

    @Override
    public @Nullable List<RelDataType> getValidatedOperandTypes(SqlCall call) {
        return parent.getValidatedOperandTypes(call);
    }

    @Override
    public void validateIdentifier(SqlIdentifier id, SqlValidatorScope scope) {
        parent.validateIdentifier(id, scope);
    }

    @Override
    public void validateLiteral(SqlLiteral literal) {
        parent.validateLiteral(literal);
    }

    @Override
    public void validateIntervalQualifier(SqlIntervalQualifier qualifier) {
        parent.validateIntervalQualifier(qualifier);
    }

    @Override
    public void validateInsert(SqlInsert insert) {
        parent.validateInsert(insert);
    }

    @Override
    public void validateUpdate(SqlUpdate update) {
        parent.validateUpdate(update);
    }

    @Override
    public void validateDelete(SqlDelete delete) {
        parent.validateDelete(delete);
    }

    @Override
    public void validateMerge(SqlMerge merge) {
        parent.validateMerge(merge);
    }

    @Override
    public void validateDataType(SqlDataTypeSpec dataType) {
        parent.validateDataType(dataType);
    }

    @Override
    public void validateDynamicParam(SqlDynamicParam dynamicParam) {
        parent.validateDynamicParam(dynamicParam);
    }

    @Override
    public void validateWindow(
            SqlNode windowOrId, SqlValidatorScope scope, @Nullable SqlCall call) {
        parent.validateWindow(windowOrId, scope, call);
    }

    @Override
    public void validateMatchRecognize(SqlCall pattern) {
        parent.validateMatchRecognize(pattern);
    }

    @Override
    public void validateCall(SqlCall call, SqlValidatorScope scope) {
        parent.validateCall(call, scope);
    }

    @Override
    public void validateAggregateParams(
            SqlCall aggCall,
            @Nullable SqlNode filter,
            @Nullable SqlNodeList distinctList,
            @Nullable SqlNodeList orderList,
            SqlValidatorScope scope) {
        parent.validateAggregateParams(aggCall, filter, distinctList, orderList, scope);
    }

    @Override
    public void validateColumnListParams(
            SqlFunction function, List<RelDataType> argTypes, List<SqlNode> operands) {
        parent.validateColumnListParams(function, argTypes, operands);
    }

    @Override
    public @Nullable SqlCall makeNullaryCall(SqlIdentifier id) {
        return parent.makeNullaryCall(id);
    }

    @Override
    public RelDataType deriveType(SqlValidatorScope scope, SqlNode operand) {
        return parent.deriveType(scope, operand);
    }

    @Override
    public CalciteContextException newValidationError(
            SqlNode node, Resources.ExInst<SqlValidatorException> e) {
        return parent.newValidationError(node, e);
    }

    @Override
    public boolean isAggregate(SqlSelect select) {
        return parent.isAggregate(select);
    }

    @Override
    public boolean isAggregate(SqlNode selectNode) {
        return parent.isAggregate(selectNode);
    }

    @Override
    public SqlWindow resolveWindow(SqlNode windowOrRef, SqlValidatorScope scope) {
        return parent.resolveWindow(windowOrRef, scope);
    }

    @Override
    public @Nullable SqlValidatorNamespace getNamespace(SqlNode node) {
        return parent.getNamespace(node);
    }

    @Override
    public @Nullable String deriveAlias(SqlNode node, int ordinal) {
        return parent.deriveAlias(node, ordinal);
    }

    @Override
    public SqlNodeList expandStar(
            SqlNodeList selectList, SqlSelect query, boolean includeSystemVars) {
        return parent.expandStar(selectList, query, includeSystemVars);
    }

    @Override
    public SqlValidatorScope getWhereScope(SqlSelect select) {
        return parent.getWhereScope(select);
    }

    @Override
    public RelDataTypeFactory getTypeFactory() {
        return parent.getTypeFactory();
    }

    @Override
    public void removeValidatedNodeType(SqlNode node) {
        parent.removeValidatedNodeType(node);
    }

    @Override
    public RelDataType getUnknownType() {
        return parent.getUnknownType();
    }

    @Override
    public SqlValidatorScope getSelectScope(SqlSelect select) {
        return parent.getSelectScope(select);
    }

    @Override
    public @Nullable SelectScope getRawSelectScope(SqlSelect select) {
        return parent.getRawSelectScope(select);
    }

    @Override
    public @Nullable SqlValidatorScope getFromScope(SqlSelect select) {
        return parent.getFromScope(select);
    }

    @Override
    public @Nullable SqlValidatorScope getJoinScope(SqlNode node) {
        return parent.getJoinScope(node);
    }

    @Override
    public SqlValidatorScope getGroupScope(SqlSelect select) {
        return parent.getGroupScope(select);
    }

    @Override
    public SqlValidatorScope getHavingScope(SqlSelect select) {
        return parent.getHavingScope(select);
    }

    @Override
    public SqlValidatorScope getOrderScope(SqlSelect select) {
        return parent.getOrderScope(select);
    }

    @Override
    public SqlValidatorScope getMatchRecognizeScope(SqlMatchRecognize node) {
        return parent.getMatchRecognizeScope(node);
    }

    @Override
    public void declareCursor(SqlSelect select, SqlValidatorScope scope) {
        parent.declareCursor(select, scope);
    }

    @Override
    public void pushFunctionCall() {
        parent.pushFunctionCall();
    }

    @Override
    public void popFunctionCall() {
        parent.popFunctionCall();
    }

    @Override
    public @Nullable String getParentCursor(String columnListParamName) {
        return parent.getParentCursor(columnListParamName);
    }

    @Override
    public RelDataType deriveConstructorType(
            SqlValidatorScope scope,
            SqlCall call,
            SqlFunction unresolvedConstructor,
            @Nullable SqlFunction resolvedConstructor,
            List<RelDataType> argTypes) {
        return parent.deriveConstructorType(
                scope, call, unresolvedConstructor, resolvedConstructor, argTypes);
    }

    @Override
    public CalciteException handleUnresolvedFunction(
            SqlCall call,
            SqlOperator unresolvedFunction,
            List<RelDataType> argTypes,
            @Nullable List<String> argNames) {
        return parent.handleUnresolvedFunction(call, unresolvedFunction, argTypes, argNames);
    }

    @Override
    public SqlNode expandOrderExpr(SqlSelect select, SqlNode orderExpr) {
        return parent.expandOrderExpr(select, orderExpr);
    }

    @Override
    public SqlNode expand(SqlNode expr, SqlValidatorScope scope) {
        return parent.expand(expr, scope);
    }

    @Override
    public boolean isSystemField(RelDataTypeField field) {
        return parent.isSystemField(field);
    }

    @Override
    public List<List<String>> getFieldOrigins(SqlNode sqlQuery) {
        return parent.getFieldOrigins(sqlQuery);
    }

    @Override
    public RelDataType getParameterRowType(SqlNode sqlQuery) {
        return parent.getParameterRowType(sqlQuery);
    }

    @Override
    public SqlValidatorScope getOverScope(SqlNode node) {
        return parent.getOverScope(node);
    }

    @Override
    public boolean validateModality(SqlSelect select, SqlModality modality, boolean fail) {
        return parent.validateModality(select, modality, fail);
    }

    @Override
    public void validateWith(SqlWith with, SqlValidatorScope scope) {
        parent.validateWith(with, scope);
    }

    @Override
    public void validateWithItem(SqlWithItem withItem) {
        parent.validateWithItem(withItem);
    }

    @Override
    public void validateSequenceValue(SqlValidatorScope scope, SqlIdentifier id) {
        parent.validateSequenceValue(scope, id);
    }

    @Override
    public @Nullable SqlValidatorScope getWithScope(SqlNode withItem) {
        return parent.getWithScope(withItem);
    }

    @Override
    public TypeCoercion getTypeCoercion() {
        return parent.getTypeCoercion();
    }

    @Override
    public Config config() {
        return parent.config();
    }

    @Override
    public SqlValidator transform(UnaryOperator<Config> transform) {
        return parent.transform(transform);
    }
}
