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
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.UUID;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

/**
 * Not a unit test: the Phase 8 generator. Replays a write plan derived from REAL Postgres change
 * events -- captured through Debezium, carrying Postgres's own commit LSNs -- into a real Iceberg
 * merge-on-read table via the stock {@link IcebergWriter} + {@link IcebergCommitter} upsert path.
 *
 * <p>The plan is a TSV of (checkpoint, id, balance, note, lsn) produced by
 * {@code phase8-cdc/oracle/build_write_plan.py}. It is replayed verbatim: this class chooses nothing
 * about ordering. The induced inversion -- the target key's later-LSN version written in an earlier
 * checkpoint than its earlier-LSN version -- lives in the plan file, so it is auditable there rather
 * than hidden in code here.
 *
 * <p>{@code lsn} is the table's ordering column. It is the real Postgres WAL position, not a counter
 * this program invents, which is what makes the resulting verdict checkable against something outside
 * the pipeline.
 *
 * <p>Run with -Dmor.out.dir=&lt;abs path&gt; -Dmor.plan=&lt;abs path to write_plan.tsv&gt;
 * [-Dmor.table=name].
 */
public class MorPhase8CdcTest {

    private static String prop(String k) {
        String v = System.getProperty(k);
        if (v == null) {
            throw new IllegalStateException("set -D" + k + "=<value>");
        }
        return v;
    }

    /** One row of the replay plan. */
    private static final class PlanRow {
        final int checkpoint;
        final long id;
        final int balance;
        final String note;
        final long lsn;

        PlanRow(int checkpoint, long id, int balance, String note, long lsn) {
            this.checkpoint = checkpoint;
            this.id = id;
            this.balance = balance;
            this.note = note;
            this.lsn = lsn;
        }
    }

    @Test
    public void generateFromRealCdc() throws Exception {
        String tableName = System.getProperty("mor.table", "phase8_cdc");
        String warehouse = new File(prop("mor.out.dir"), tableName + "_wh").toString();
        deleteRecursively(new File(warehouse));

        List<PlanRow> plan = readPlan(prop("mor.plan"));
        if (plan.isEmpty()) {
            throw new IllegalStateException(
                    "write plan is empty; an empty replay would produce an empty table and every "
                            + "later check would pass vacuously");
        }
        TreeSet<Integer> checkpoints = new TreeSet<>();
        for (PlanRow r : plan) {
            checkpoints.add(r.checkpoint);
        }

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

        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.BIGINT().notNull(), "pk", null)
                        .physicalColumn("balance", DataTypes.INT(), "balance", null)
                        .physicalColumn("note", DataTypes.VARCHAR(255), "note", null)
                        // the ordering column: Postgres's own commit LSN, carried through Debezium
                        .physicalColumn("lsn", DataTypes.BIGINT(), "postgres commit lsn", null)
                        .primaryKey("id")
                        .build();
        CreateTableEvent createTableEvent = new CreateTableEvent(tableId, schema);
        applier.applySchemaChange(createTableEvent);
        writer.write(createTableEvent, null);

        DataType[] types =
                createTableEvent.getSchema().getColumnDataTypes().toArray(new DataType[0]);
        BinaryRecordDataGenerator gen = new BinaryRecordDataGenerator(types);

        for (int cp : checkpoints) {
            int n = 0;
            for (PlanRow r : plan) {
                if (r.checkpoint != cp) {
                    continue;
                }
                Object[] vals =
                        new Object[] {
                            r.id, r.balance, BinaryStringData.fromString(r.note), r.lsn
                        };
                RecordData rd = gen.generate(vals);
                writer.write(DataChangeEvent.insertEvent(tableId, rd), null);
                n++;
            }
            commit(writer, catalogOptions);
            System.out.println("[phase8] checkpoint " + cp + " committed, " + n + " row(s)");
        }

        writer.close();
        System.out.println("[phase8] wrote " + tableName + " to " + warehouse);
    }

    private static List<PlanRow> readPlan(String path) throws Exception {
        List<PlanRow> out = new ArrayList<>();
        List<String> lines = Files.readAllLines(Paths.get(path), StandardCharsets.UTF_8);
        for (int i = 0; i < lines.size(); i++) {
            String line = lines.get(i).trim();
            if (line.isEmpty() || i == 0) {
                continue; // header
            }
            String[] f = line.split("\t", -1);
            out.add(
                    new PlanRow(
                            Integer.parseInt(f[0]),
                            Long.parseLong(f[1]),
                            Integer.parseInt(f[2]),
                            f[3],
                            Long.parseLong(f[4])));
        }
        return out;
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
        File[] kids = f.listFiles();
        if (kids != null) {
            for (File k : kids) {
                deleteRecursively(k);
            }
        }
        if (!f.delete()) {
            throw new IllegalStateException("could not delete " + f);
        }
    }
}
