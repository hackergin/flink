package org.apache.flink.table.planner.plan.abilities.source;

import org.apache.flink.table.connector.source.DynamicTableSource;

public class TimeTravelSpec extends SourceAbilitySpecBase{
    @Override
    public void apply(DynamicTableSource tableSource, SourceAbilityContext context) {

    }

    @Override
    public String getDigests(SourceAbilityContext context) {
        return null;
    }
}
