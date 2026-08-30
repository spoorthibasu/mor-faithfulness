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
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

/**
 * Phase 8, parallel-sink variant: does the pipeline reorder BY ITSELF when configured the way
 * FLINK-20374 describes, rather than having the inversion assigned in a write plan?
 *
 * <p>FLINK-20374 ("Wrong result when shuffling changelog stream on non-primary-key columns") reports
 * that when a key's change events are shuffled on a non-key column they reach different subtasks and
 * "the order of joined results is not guaranteed when they arrive to the sink task". So this runs the
 * sink at parallelism 2 and assigns each event to a subtask by hashing {@code note} -- a NON-key
 * column -- which sends key 42's two final versions to different writers.
 *
 * <p>THIS IS A RACE, NOT A REPLAY, and it is built to be one. Each subtask writes on its own thread
 * with jitter; a coordinator thread fires checkpoint barriers on a timer. Which events have been
 * written when a barrier fires -- and therefore which Iceberg snapshot, and which sequence number,
 * they land in -- depends on thread scheduling. Nothing here assigns key 42's versions to
 * checkpoints. If the two versions land in different checkpoints in inverted LSN order, the pipeline
 * produced the inversion on its own. If they land in the same checkpoint, they share a sequence
 * number and the result is the DUPLICATE shape instead. If they land in order, the table is faithful.
 *
 * <p>Each writer is guarded by its own lock so that a barrier's prepareCommit() never runs
 * concurrently with that writer's own write(). That protects the writer's internal state without
 * constraining the thing under test: WHICH events precede a barrier remains timing-dependent.
 *
 * <p>Expected outcome is that this does not reproduce on demand. A negative is a real answer here.
 */
public class MorPhase8ParallelTest {

    private static final int PARALLELISM = 2;

    private static String prop(String k) {
        String v = System.getProperty(k);
        if (v == null) {
            throw new IllegalStateException("set -D" + k + "=<value>");
        }
        return v;
    }

    private static final class Ev {
        final long id;
        final int balance;
        final String note;
        final long lsn;

        Ev(long id, int balance, String note, long lsn) {
            this.id = id;
            this.balance = balance;
            this.note = note;
            this.lsn = lsn;
        }
    }

    @Test
    public void parallelSinkRace() throws Exception {
        String tableName = System.getProperty("mor.table", "phase8_parallel");
        String warehouse = new File(prop("mor.out.dir"), tableName + "_wh").toString();
        deleteRecursively(new File(warehouse));

        List<Ev> events = readEvents(prop("mor.events"));
        int barriers = Integer.parseInt(System.getProperty("mor.barriers", "6"));
        long barrierMs = Long.parseLong(System.getProperty("mor.barrier.ms", "40"));
        long jitterMs = Long.parseLong(System.getProperty("mor.jitter.ms", "3"));

        Map<String, String> catalogOptions = new HashMap<>();
        catalogOptions.put("type", "hadoop");
        catalogOptions.put("warehouse", warehouse);
        catalogOptions.put("cache-enabled", "false");

        String jobId = UUID.randomUUID().toString();
        String operatorId = UUID.randomUUID().toString();

        IcebergMetadataApplier applier = new IcebergMetadataApplier(catalogOptions);
        TableId tableId = TableId.parse("realworld." + tableName);
        Schema schema =
                Schema.newBuilder()
                        .physicalColumn("id", DataTypes.BIGINT().notNull(), "pk", null)
                        .physicalColumn("balance", DataTypes.INT(), "balance", null)
                        .physicalColumn("note", DataTypes.VARCHAR(255), "note", null)
                        .physicalColumn("lsn", DataTypes.BIGINT(), "postgres commit lsn", null)
                        .primaryKey("id")
                        .build();
        CreateTableEvent createTableEvent = new CreateTableEvent(tableId, schema);
        applier.applySchemaChange(createTableEvent);

        IcebergWriter[] writers = new IcebergWriter[PARALLELISM];
        Object[] locks = new Object[PARALLELISM];
        for (int i = 0; i < PARALLELISM; i++) {
            writers[i] =
                    new IcebergWriter(
                            catalogOptions, i, 1, ZoneId.systemDefault(), 0, jobId, operatorId,
                            new HashMap<>());
            writers[i].write(createTableEvent, null);
            locks[i] = new Object();
        }

        DataType[] types =
                createTableEvent.getSchema().getColumnDataTypes().toArray(new DataType[0]);

        // Partition on `note`, a NON-primary-key column: this is the FLINK-20374 configuration.
        List<List<Ev>> perSubtask = new ArrayList<>();
        for (int i = 0; i < PARALLELISM; i++) {
            perSubtask.add(new ArrayList<>());
        }
        for (Ev e : events) {
            int st = Math.floorMod(e.note.hashCode(), PARALLELISM);
            perSubtask.get(st).add(e);
        }
        for (int i = 0; i < PARALLELISM; i++) {
            System.out.println("[parallel] subtask " + i + " gets " + perSubtask.get(i).size()
                    + " event(s)");
        }

        CountDownLatch done = new CountDownLatch(PARALLELISM);
        for (int i = 0; i < PARALLELISM; i++) {
            final int st = i;
            Thread t =
                    new Thread(
                            () -> {
                                BinaryRecordDataGenerator gen =
                                        new BinaryRecordDataGenerator(types);
                                try {
                                    for (Ev e : perSubtask.get(st)) {
                                        RecordData rd =
                                                gen.generate(
                                                        new Object[] {
                                                            e.id,
                                                            e.balance,
                                                            BinaryStringData.fromString(e.note),
                                                            e.lsn
                                                        });
                                        synchronized (locks[st]) {
                                            writers[st].write(
                                                    DataChangeEvent.insertEvent(tableId, rd), null);
                                        }
                                        if (jitterMs > 0) {
                                            Thread.sleep(jitterMs);
                                        }
                                    }
                                } catch (Exception ex) {
                                    throw new RuntimeException(ex);
                                } finally {
                                    done.countDown();
                                }
                            },
                            "subtask-" + st);
            t.start();
        }

        // Coordinator: fire barriers on a timer while the subtasks are still writing.
        int cp = 0;
        while (cp < barriers && done.getCount() > 0) {
            Thread.sleep(barrierMs);
            cp++;
            commitAll(writers, locks, catalogOptions, cp);
        }
        done.await();
        cp++;
        commitAll(writers, locks, catalogOptions, cp); // final barrier drains whatever is left

        for (IcebergWriter w : writers) {
            w.close();
        }
        System.out.println("[parallel] wrote " + tableName + " to " + warehouse
                + " over " + cp + " barrier(s)");
    }

    private static void commitAll(
            IcebergWriter[] writers, Object[] locks, Map<String, String> catalogOptions, int cp)
            throws Exception {
        java.util.Collection<Committer.CommitRequest<WriteResultWrapper>> all = new ArrayList<>();
        for (int i = 0; i < writers.length; i++) {
            synchronized (locks[i]) {
                all.addAll(
                        writers[i].prepareCommit().stream()
                                .map(IcebergWriterTest.MockCommitRequestImpl::new)
                                .collect(Collectors.toList()));
            }
        }
        if (all.isEmpty()) {
            return;
        }
        try (IcebergCommitter committer = new IcebergCommitter(catalogOptions, new HashMap<>())) {
            committer.commit(all);
        }
        System.out.println("[parallel] barrier " + cp + " committed " + all.size() + " result(s)");
    }

    private static List<Ev> readEvents(String path) throws Exception {
        List<Ev> out = new ArrayList<>();
        List<String> lines = Files.readAllLines(Paths.get(path), StandardCharsets.UTF_8);
        for (int i = 1; i < lines.size(); i++) {
            String line = lines.get(i).trim();
            if (line.isEmpty()) {
                continue;
            }
            String[] f = line.split("\t", -1);
            // columns: id, balance, note, lsn  (already in Postgres LSN order)
            out.add(
                    new Ev(
                            Long.parseLong(f[0]),
                            Integer.parseInt(f[1]),
                            f[2],
                            Long.parseLong(f[3])));
        }
        return out;
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
