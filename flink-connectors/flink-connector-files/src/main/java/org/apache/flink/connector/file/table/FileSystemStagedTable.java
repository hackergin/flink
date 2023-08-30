package org.apache.flink.connector.file.table;

import org.apache.flink.core.fs.Path;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.StagedTable;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;

public class FileSystemStagedTable implements StagedTable {

    private static final long serialVersionUID = 1L;

    private FileSystemFactory fsFactory;
    private TableMetaStoreFactory msFactory;
    private boolean overwrite;
    private Path tmpPath;
    private String[] partitionColumns;
    private boolean dynamicGrouped;
    private LinkedHashMap<String, String> staticPartitions;
    private ObjectIdentifier identifier;
    private PartitionCommitPolicyFactory partitionCommitPolicyFactory;

    @Override
    public void begin() {

    }

    @Override
    public void commit() {

        try {
            List<PartitionCommitPolicy> policies = Collections.emptyList();
            policies =
                    partitionCommitPolicyFactory.createPolicyChain(
                            Thread.currentThread().getContextClassLoader(),
                            () -> {
                                try {
                                    return fsFactory.create(tmpPath.toUri());
                                } catch (IOException e) {
                                    throw new RuntimeException(e);
                                }
                            });
            FileSystemCommitter committer = new FileSystemCommitter(
                    fsFactory,
                    msFactory,
                    overwrite,
                    tmpPath,
                    partitionColumns.length,
                    false,
                    identifier,
                    staticPartitions,
                    policies);

            committer.commitPartitions();
        } catch (Exception e) {

        }
    }

    @Override
    public void abort() {

    }

    public void setFsFactory(FileSystemFactory fsFactory) {
        this.fsFactory = fsFactory;
    }

    public void setMsFactory(TableMetaStoreFactory msFactory) {
        this.msFactory = msFactory;
    }

    public void setOverwrite(boolean overwrite) {
        this.overwrite = overwrite;
    }

    public void setTmpPath(Path tmpPath) {
        this.tmpPath = tmpPath;
    }

    public void setPartitionColumns(String[] partitionColumns) {
        this.partitionColumns = partitionColumns;
    }

    public void setDynamicGrouped(boolean dynamicGrouped) {
        this.dynamicGrouped = dynamicGrouped;
    }

    public void setStaticPartitions(LinkedHashMap<String, String> staticPartitions) {
        this.staticPartitions = staticPartitions;
    }

    public void setIdentifier(ObjectIdentifier identifier) {
        this.identifier = identifier;
    }

    public void setPartitionCommitPolicyFactory(
            PartitionCommitPolicyFactory partitionCommitPolicyFactory) {
        this.partitionCommitPolicyFactory = partitionCommitPolicyFactory;
    }
}
