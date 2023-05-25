package org.apache.flink.table.planner.plan;

import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.SchemaVersion;
import org.apache.calcite.sql.validate.SqlValidatorCatalogReader;

public class FlinkCalciteCatalogReaderSnapshot extends FlinkCalciteCatalogReader {

    private SchemaVersion schemaVersion;

    public FlinkCalciteCatalogReaderSnapshot(
            SqlValidatorCatalogReader calciteCatalogReader,
            RelDataTypeFactory typeFactory,
            SchemaVersion schemaVersion) {
        super(
                calciteCatalogReader.getRootSchema().createSnapshot(schemaVersion),
                calciteCatalogReader.getSchemaPaths(),
                typeFactory,
                calciteCatalogReader.getConfig());
        this.schemaVersion = schemaVersion;
    }
}
