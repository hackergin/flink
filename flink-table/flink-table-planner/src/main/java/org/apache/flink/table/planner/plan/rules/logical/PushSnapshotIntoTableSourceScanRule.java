package org.apache.flink.table.planner.plan.rules.logical;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;

import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.logical.LogicalSnapshot;

import org.apache.calcite.rel.logical.LogicalTableScan;

import org.apache.calcite.rex.RexLiteral;

import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalSnapshot;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalSort;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalTableSourceScan;
import org.apache.flink.table.planner.plan.schema.TableSourceTable;

public class PushSnapshotIntoTableSourceScanRule extends RelOptRule {
    public static final PushSnapshotIntoTableSourceScanRule INSTANCE =
            new PushSnapshotIntoTableSourceScanRule();
    public PushSnapshotIntoTableSourceScanRule() {
        super(
                operand(FlinkLogicalSnapshot.class, operand(FlinkLogicalTableSourceScan.class, none())),
                "PushSnapshotIntoTableSourceScanRule");
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        FlinkLogicalSnapshot snapshot = call.rel(0);
        FlinkLogicalTableSourceScan scan = call.rel(1);

        Comparable comparable = RexLiteral.value(snapshot.getPeriod());

        return comparable != null;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        FlinkLogicalSnapshot snapshot = call.rel(0);
        FlinkLogicalTableSourceScan scan = call.rel(1);

        snapshot.getPeriod();

        TableSourceTable tableSourceTable = scan.getTable().unwrap(TableSourceTable.class);

//        tableSourceTable.abilitySpecs();

        LogicalTableScan newScan =
                LogicalTableScan.create(scan.getCluster(), tableSourceTable, scan.getHints());
        call.transformTo(newScan);
    }
}
