package org.example.rocksdbrw;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class RocksDBReadWriteJob {
    public static final String OPERATOR_NAME_ID = "UpdateState";

    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);

        String jobName = params.get("job-name", "RocksDBReadWrite");
        int maxId = params.getInt("max-id", 1_000_000);
        int maxKey = params.getInt("max-key", 200);
        boolean useValueState = params.getBoolean("use-value-state", false);
        boolean enableBloomFilter = params.getBoolean("enable-bloom-filter", false);
        boolean indexFilterInCache = params.getBoolean("index-filter-in-cache", false);
        boolean aggressiveCompaction = params.getBoolean("aggressive-compaction", false);
        boolean useFsStateBackend = params.getBoolean("use-fs-state-backend", false);
        boolean useIncrementalCheckpointing = params.getBoolean("use-incremental-checkpointing", true);
        String checkpointUrl = params.get("checkpoint-url", "file:///tmp");
        int valueLength = params.getInt("value-length", -1);
        int sleepMs = params.getInt("sleep-ms", 0);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        if (!useFsStateBackend) {
            RocksDBStateBackend stateBackend = new RocksDBStateBackend(checkpointUrl, useIncrementalCheckpointing);
            stateBackend.setRocksDBOptions(new CustomRocksDBOptionsFactory(enableBloomFilter, indexFilterInCache, aggressiveCompaction));
            env.setStateBackend(stateBackend);
        }

        if (useValueState) {
            if (valueLength > 0) {
                env.addSource(new LargeAlarmSource(maxId, maxKey, valueLength, sleepMs))
                    .keyBy(e -> e.id)
                    .process(new LargeValueStateProcessFunction())
                    .name(OPERATOR_NAME_ID).uid(OPERATOR_NAME_ID);
            } else {
                env.addSource(new AlarmSource(maxId, maxKey, sleepMs))
                    .keyBy(e -> e.id)
                    .process(new ValueStateProcessFunction())
                    .name(OPERATOR_NAME_ID).uid(OPERATOR_NAME_ID);
            }
        } else {
            if (valueLength > 0) {
                env.addSource(new LargeAlarmSource(maxId, maxKey, valueLength, sleepMs))
                    .keyBy(e -> e.id)
                    .process(new LargeMapStateProcessFunction())
                    .name(OPERATOR_NAME_ID).uid(OPERATOR_NAME_ID);
            } else {
                env.addSource(new AlarmSource(maxId, maxKey, sleepMs))
                    .keyBy(e -> e.id)
                    .process(new MapStateProcessFunction())
                    .name(OPERATOR_NAME_ID).uid(OPERATOR_NAME_ID);
            }

        }

        env.execute(jobName);
    }
}
