package com.pipeline.common;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

public final class Checkpointing {

    private Checkpointing() {}

    public static void configure(StreamExecutionEnvironment env) {
        env.enableCheckpointing(60_000, CheckpointingMode.EXACTLY_ONCE);
        CheckpointConfig cc = env.getCheckpointConfig();

        // A failed checkpoint must not restart the job: every restart
        // re-emits events since the last completed checkpoint through the
        // at-least-once Kafka sink (June 2026 incident: a restore loop
        // duplicated 2.6x of prod data). Only restart after ~15 minutes
        // of consecutive failures, when storage is genuinely gone.
        cc.setTolerableCheckpointFailureNumber(15);

        // Under backlog reprocessing the pipeline is backpressured for
        // hours; aligned barriers can stall past the checkpoint timeout
        // and fail the checkpoint. Start aligned, switch to unaligned
        // after 30s so backpressure cannot fail checkpoints indefinitely.
        cc.enableUnalignedCheckpoints();
        cc.setAlignedCheckpointTimeout(Duration.ofSeconds(30));
    }
}
