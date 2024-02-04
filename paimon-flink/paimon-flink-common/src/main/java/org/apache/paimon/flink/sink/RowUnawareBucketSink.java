/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.flink.sink;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.flink.compact.UnawareBucketCompactionTopoBuilder;
import org.apache.paimon.table.FileStoreTable;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;

import javax.annotation.Nullable;

import java.util.Map;

/** An {@link UnawareBucketSink} which handles {@link InternalRow}. */
public class RowUnawareBucketSink extends UnawareBucketSink<InternalRow> {

    public RowUnawareBucketSink(
            FileStoreTable table,
            Map<String, String> overwritePartitions,
            LogSinkFunction logSinkFunction,
            Integer parallelism,
            boolean boundedInput) {
        super(table, overwritePartitions, logSinkFunction, parallelism, boundedInput);
    }

    @Override
    protected OneInputStreamOperator<InternalRow, Committable> createWriteOperator(
            StoreSinkWrite.Provider writeProvider, String commitUser) {
        return new RowDataStoreWriteOperator(table, logSinkFunction, writeProvider, commitUser);
    }

    @Override
    public DataStream<Committable> doWrite(
            DataStream<InternalRow> input,
            String initialCommitUser,
            @Nullable Integer parallelism) {
        DataStream<Committable> written = super.doWrite(input, initialCommitUser, parallelism);
        boolean enableCompaction = !table.coreOptions().writeOnly();
        boolean isStreamingMode =
                input.getExecutionEnvironment()
                                .getConfiguration()
                                .get(ExecutionOptions.RUNTIME_MODE)
                        == RuntimeExecutionMode.STREAMING;
        // if enable compaction, we need to add compaction topology to this job
        if (enableCompaction && isStreamingMode && !boundedInput) {
            // if streaming mode with bounded input, we disable compaction topology
            UnawareBucketCompactionTopoBuilder builder =
                    new UnawareBucketCompactionTopoBuilder(
                            input.getExecutionEnvironment(), table.name(), table);
            builder.withContinuousMode(true);
            written = written.union(builder.fetchUncommitted(initialCommitUser));
        }
        return written;
    }
}
