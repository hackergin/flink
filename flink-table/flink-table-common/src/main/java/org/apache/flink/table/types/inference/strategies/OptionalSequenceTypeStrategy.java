package org.apache.flink.table.types.inference.strategies;

import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.ArgumentCount;
import org.apache.flink.table.types.inference.ArgumentTypeStrategy;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.ConstantArgumentCount;
import org.apache.flink.table.types.inference.InputTypeStrategy;
import org.apache.flink.table.types.inference.Signature;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class OptionalSequenceTypeStrategy implements InputTypeStrategy {

    private final List<? extends ArgumentTypeStrategy> argumentStrategies;

    private final @Nullable List<String> argumentNames;

    private final @Nullable List<Boolean> argumentOptionals;

    public OptionalSequenceTypeStrategy(
            List<? extends ArgumentTypeStrategy> argumentStrategies,
            @Nullable List<String> argumentNames,
            @Nullable List<Boolean> argumentOptionals) {
        Preconditions.checkArgument(
                argumentNames == null || argumentNames.size() == argumentStrategies.size());
        this.argumentStrategies = argumentStrategies;
        this.argumentNames = argumentNames;
        this.argumentOptionals = argumentOptionals;
    }

    @Override
    public ArgumentCount getArgumentCount() {
        if (argumentOptionals != null) {
            long optionalCount = argumentOptionals.stream().filter(Boolean::booleanValue).count();
            return ConstantArgumentCount.between(
                    argumentStrategies.size() - (int) optionalCount, argumentStrategies.size());

        } else {
            return ConstantArgumentCount.of(argumentStrategies.size());
        }
    }

    @Override
    public Optional<List<DataType>> inferInputTypes(
            CallContext callContext, boolean throwOnFailure) {
        final List<DataType> dataTypes = callContext.getArgumentDataTypes();
        if (dataTypes.size() != argumentStrategies.size()) {
            return Optional.empty();
        }
        final List<DataType> inferredDataTypes = new ArrayList<>(dataTypes.size());
        for (int i = 0; i < argumentStrategies.size(); i++) {
            final ArgumentTypeStrategy argumentTypeStrategy = argumentStrategies.get(i);
            final Optional<DataType> inferredDataType =
                    argumentTypeStrategy.inferArgumentType(callContext, i, throwOnFailure);
            if (!inferredDataType.isPresent()) {
                return Optional.empty();
            }
            inferredDataTypes.add(inferredDataType.get());
        }
        return Optional.of(inferredDataTypes);
    }

    @Override
    public List<Signature> getExpectedSignatures(FunctionDefinition definition) {
        final List<Signature.Argument> arguments = new ArrayList<>();
        for (int i = 0; i < argumentStrategies.size(); i++) {
            if (argumentNames == null) {
                arguments.add(argumentStrategies.get(i).getExpectedArgument(definition, i));
            } else {
                arguments.add(
                        Signature.Argument.of(
                                argumentNames.get(i),
                                argumentStrategies
                                        .get(i)
                                        .getExpectedArgument(definition, i)
                                        .getType()));
            }
        }

        return Collections.singletonList(Signature.of(arguments));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        OptionalSequenceTypeStrategy that = (OptionalSequenceTypeStrategy) o;
        return Objects.equals(argumentStrategies, that.argumentStrategies)
                && Objects.equals(argumentNames, that.argumentNames);
    }

    @Override
    public int hashCode() {
        return Objects.hash(argumentStrategies, argumentNames);
    }
}
