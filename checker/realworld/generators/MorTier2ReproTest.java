/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cdc.connectors.iceberg.sink.v2;

import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.data.binary.BinaryStringData;
import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.PhysicalColumn;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.utils.SchemaUtils;
import org.apache.flink.cdc.connectors.iceberg.sink.IcebergMetadataApplier;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;

import java.io.File;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

/**
 * FLINK-38450 reproduction driver. Within ONE checkpoint, upserts the SAME key in two batches
 * split by a mid-checkpoint schema change (mimicking the framework's pre-schema-change flush via
 * flush(false)). On the unmodified pre-fix connector the two batches merge into one Iceberg
 * snapshot, so the equality delete shares a sequence number with the data and fails to suppress
 * the stale version, leaving two rows for the key. A monotonic op_seq column lets the checker
 * certify the post-fix survivor as FAITHFUL. Output dir from -Dmor.out.dir, table name from
 * -Dmor.table. Drives the real IcebergWriter/IcebergCommitter; no connector code is modified.
 */
public class MorTier2ReproTest {

    @Test
    public void reproduceSameCheckpointSchemaSplit() throws Exception {
        String out = System.getProperty("mor.out.dir");
        String tableName = System.getProperty("mor.table", "t2_repro");
        if (out == null) {
            throw new IllegalStateException("set -Dmor.out.dir");
        }
        String warehouse = new File(out, tableName + "_wh").toString();
        deleteRecursively(new File(warehouse));

        Map<String, String> catalogOptions = new HashMap<>();
        catalogOptions.put("type", "hadoop");
        catalogOptions.put("warehouse", warehouse);
        catalogOptions.put("cache-enabled", "false");

        IcebergWriter writer =
                new IcebergWriter(
                        catalogOptions, 1, 1, java.time.ZoneId.systemDefault(), 0,
                        UUID.randomUUID().toString(), UUID.randomUUID().toString(), new HashMap<>());
        IcebergMetadataApplier applier = new IcebergMetadataApplier(catalogOptions);
        TableId tableId = TableId.parse("realworld." + tableName);

        CreateTableEvent createTableEvent =
                new CreateTableEvent(
                        tableId,
                        Schema.newBuilder()
                                .physicalColumn("id", DataTypes.BIGINT().notNull(), "pk", null)
                                .physicalColumn("name", DataTypes.VARCHAR(255), "name", null)
                                .physicalColumn("val", DataTypes.INT(), "value", null)
                                .physicalColumn("op_seq", DataTypes.BIGINT(), "monotonic", null)
                                .primaryKey("id")
                                .build());
        applier.applySchemaChange(createTableEvent);
        writer.write(createTableEvent, null);

        DataType[] t0 =
                createTableEvent.getSchema().getColumnDataTypes().toArray(new DataType[0]);
        BinaryRecordDataGenerator gen0 = new BinaryRecordDataGenerator(t0);

        // ---- Single checkpoint ----
        // batch 1: upsert key id=1 -> "v1" (old schema), op_seq=1
        RecordData r1 =
                gen0.generate(new Object[] {1L, BinaryStringData.fromString("v1"), 100, 1L});
        writer.write(DataChangeEvent.insertEvent(tableId, r1), null);

        // framework flushes the affected table before applying the schema change
        writer.flush(false);

        // mid-checkpoint schema change
        AddColumnEvent addColumnEvent =
                new AddColumnEvent(
                        tableId,
                        Arrays.asList(
                                AddColumnEvent.last(
                                        new PhysicalColumn(
                                                "extra", DataTypes.STRING(), "added", null))));
        applier.applySchemaChange(addColumnEvent);
        writer.write(addColumnEvent, null);

        // batch 2: upsert the SAME key id=1 -> "v2" (new schema), op_seq=2 (current version)
        DataType[] t1 =
                SchemaUtils.applySchemaChangeEvent(createTableEvent.getSchema(), addColumnEvent)
                        .getColumnDataTypes()
                        .toArray(new DataType[0]);
        BinaryRecordDataGenerator gen1 = new BinaryRecordDataGenerator(t1);
        RecordData r2 =
                gen1.generate(
                        new Object[] {
                            1L,
                            BinaryStringData.fromString("v2"),
                            200,
                            2L,
                            BinaryStringData.fromString("x")
                        });
        writer.write(DataChangeEvent.insertEvent(tableId, r2), null);

        // one commit for the whole checkpoint
        Collection<Committer.CommitRequest<WriteResultWrapper>> collection =
                writer.prepareCommit().stream()
                        .map(IcebergWriterTest.MockCommitRequestImpl::new)
                        .collect(Collectors.toList());
        try (IcebergCommitter committer = new IcebergCommitter(catalogOptions, new HashMap<>())) {
            committer.commit(collection);
        }
        writer.close();
        System.out.println("[mor-tier2] wrote " + tableName + " to " + warehouse);
    }

    private static void deleteRecursively(File f) throws Exception {
        if (!f.exists()) {
            return;
        }
        Files.walk(f.toPath())
                .sorted(Comparator.reverseOrder())
                .forEach(
                        p -> {
                            try {
                                Files.delete(p);
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        });
    }
}
