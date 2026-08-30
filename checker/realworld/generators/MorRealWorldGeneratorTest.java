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
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.connectors.iceberg.sink.IcebergMetadataApplier;
import org.apache.flink.cdc.runtime.typeutils.BinaryRecordDataGenerator;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.ZoneId;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

/**
 * Not a unit test: a generator that drives the real {@link IcebergWriter} + {@link
 * IcebergCommitter} (upsert=true, the reference iceberg-flink writer) with a synthetic CDC upsert
 * workload, producing a real on-disk equality-delete Iceberg table for the mor_checker real-world
 * evaluation. Output dir comes from -Dmor.out.dir. Two variants: plain (no version column) and
 * op_seq (monotonic version column). Provenance: real-writer-generated, synthetic workload.
 */
public class MorRealWorldGeneratorTest {

    private static String outDir() {
        String d = System.getProperty("mor.out.dir");
        if (d == null) {
            throw new IllegalStateException("set -Dmor.out.dir=<abs path>");
        }
        return d;
    }

    @Test
    public void generatePlainUpsertTable() throws Exception {
        runWorkload("upsert_plain", false);
    }

    @Test
    public void generateOpSeqUpsertTable() throws Exception {
        runWorkload("upsert_opseq", true);
    }

    private void runWorkload(String tableName, boolean withOpSeq) throws Exception {
        String warehouse = new File(outDir(), tableName + "_wh").toString();
        deleteRecursively(new File(warehouse));

        Map<String, String> catalogOptions = new HashMap<>();
        catalogOptions.put("type", "hadoop");
        catalogOptions.put("warehouse", warehouse);
        catalogOptions.put("cache-enabled", "false");

        String jobId = UUID.randomUUID().toString();
        String operatorId = UUID.randomUUID().toString();

        IcebergWriter writer =
                new IcebergWriter(
                        catalogOptions, 1, 1, ZoneId.systemDefault(), 0, jobId, operatorId,
                        new HashMap<>());
        IcebergMetadataApplier applier = new IcebergMetadataApplier(catalogOptions);
        TableId tableId = TableId.parse("realworld." + tableName);

        Schema.Builder sb =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.BIGINT().notNull(), "pk", null)
                        .physicalColumn("name", DataTypes.VARCHAR(255), "name", null)
                        .physicalColumn("val", DataTypes.INT(), "value", null);
        if (withOpSeq) {
            sb.physicalColumn("op_seq", DataTypes.BIGINT(), "monotonic version", null);
        }
        CreateTableEvent createTableEvent =
                new CreateTableEvent(tableId, sb.primaryKey("id").build());
        applier.applySchemaChange(createTableEvent);
        writer.write(createTableEvent, null);

        DataType[] types =
                createTableEvent.getSchema().getColumnDataTypes().toArray(new DataType[0]);
        BinaryRecordDataGenerator gen = new BinaryRecordDataGenerator(types);
        long[] op = {0};

        // checkpoint 1: three inserts
        writer.write(row(gen, tableId, 1L, "a", 10, withOpSeq, op), null);
        writer.write(row(gen, tableId, 2L, "b", 20, withOpSeq, op), null);
        writer.write(row(gen, tableId, 3L, "c", 30, withOpSeq, op), null);
        commit(writer, catalogOptions);

        // checkpoint 2: upsert id=1, id=2 (same PK, new value -> equality delete + new data)
        writer.write(row(gen, tableId, 1L, "a", 11, withOpSeq, op), null);
        writer.write(row(gen, tableId, 2L, "b", 21, withOpSeq, op), null);
        commit(writer, catalogOptions);

        // checkpoint 3: upsert id=1, id=3
        writer.write(row(gen, tableId, 1L, "a", 12, withOpSeq, op), null);
        writer.write(row(gen, tableId, 3L, "c", 31, withOpSeq, op), null);
        commit(writer, catalogOptions);

        writer.close();
        System.out.println("[mor-realworld] wrote " + tableName + " to " + warehouse);
    }

    private static DataChangeEvent row(
            BinaryRecordDataGenerator gen,
            TableId tableId,
            long id,
            String name,
            int val,
            boolean withOpSeq,
            long[] op) {
        Object[] vals =
                withOpSeq
                        ? new Object[] {id, BinaryStringData.fromString(name), val, ++op[0]}
                        : new Object[] {id, BinaryStringData.fromString(name), val};
        RecordData rd = gen.generate(vals);
        return DataChangeEvent.insertEvent(tableId, rd);
    }

    private static void commit(IcebergWriter writer, Map<String, String> catalogOptions)
            throws Exception {
        java.util.Collection<Committer.CommitRequest<WriteResultWrapper>> collection =
                writer.prepareCommit().stream()
                        .map(IcebergWriterTest.MockCommitRequestImpl::new)
                        .collect(Collectors.toList());
        try (IcebergCommitter committer = new IcebergCommitter(catalogOptions, new HashMap<>())) {
            committer.commit(collection);
        }
    }

    private static void deleteRecursively(File f) throws Exception {
        if (!f.exists()) {
            return;
        }
        Path root = f.toPath();
        Files.walk(root)
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
