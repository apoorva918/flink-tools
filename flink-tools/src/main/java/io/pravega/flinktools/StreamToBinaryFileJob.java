/*
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 */
package io.pravega.flinktools;

import io.pravega.client.stream.StreamCut;
import io.pravega.connectors.flink.FlinkPravegaReader;
import io.pravega.flinktools.util.ByteArrayDeserializationFormat;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Copy a Pravega stream to a set of files on any Flink file system, including S3.
 * This uses Flink to provide exactly-once, recovery from failures, and parallelism.
 * The stream is assumed to contain UTF-8 strings.
 * When written to files, each event will be followed by a new line.
 */
public class StreamToBinaryFileJob extends AbstractJob {
    final private static Logger log = LoggerFactory.getLogger(StreamToBinaryFileJob.class);

    /**
     * The entry point for Flink applications.
     *
     * @param args Command line arguments
     */
    public static void main(String... args) throws Exception {
        AppConfiguration config = new AppConfiguration(args);
        log.info("config: {}", config);
        StreamToBinaryFileJob job = new StreamToBinaryFileJob(config);
        job.run();
    }

    public StreamToBinaryFileJob(AppConfiguration appConfiguration) {
        super(appConfiguration);
    }

    public void run() {
        try {
            final String jobName = getConfig().getJobName(StreamToBinaryFileJob.class.getName());
            final AppConfiguration.StreamConfig inputStreamConfig = getConfig().getStreamConfig("input");
            log.info("input stream: {}", inputStreamConfig);
            final String outputFilePath = getConfig().getParams().getRequired("output");
            log.info("output file: {}", outputFilePath);
            createStream(inputStreamConfig);
            final StreamCut startStreamCut = resolveStartStreamCut(inputStreamConfig);
            final StreamCut endStreamCut = resolveEndStreamCut(inputStreamConfig);
            final StreamExecutionEnvironment env = initializeFlinkStreaming();

            final FlinkPravegaReader<byte[]> flinkPravegaReader = FlinkPravegaReader.<byte[]>builder()
                    .withPravegaConfig(inputStreamConfig.getPravegaConfig())
                    .forStream(inputStreamConfig.getStream(), startStreamCut, endStreamCut)
                    .withDeserializationSchema(new ByteArrayDeserializationFormat())
                    .build();
                    
            final DataStream<byte[]> lines = env
                    .addSource(flinkPravegaReader)
                    .uid("pravega-reader")
                    .name("pravega-reader");

            //Each event is a binary file so every line will have ingested binary file size
            final DataStream<String> byteSizes = lines.flatMap((byte[] value, Collector<String> out) -> {
                                                                                        int size = value.length;
                                                                                        out.collect(Integer.toString(size));
                                                                });

            // final DataStream<byte[]> toOutput = Filters.dynamicByteArrayFilter(lines, getConfig().getParams());
            
            final StreamingFileSink<String> sink = StreamingFileSink
                    .forRowFormat(new Path(outputFilePath), new SimpleStringEncoder<String>())
                    .withRollingPolicy(OnCheckpointRollingPolicy.build())
                    .build();
            byteSizes.addSink(sink)
                .uid("file-sink")
                .name("file-sink");

            log.info("Executing {} job", jobName);
            env.execute(jobName);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
