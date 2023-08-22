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

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Read a Pravega stream with a binary files and display their sizes in the Task Manager stderr.
 * Each file is one event.
 */
public class StreamToBinarySizeJob extends AbstractJob {
    final private static Logger log = LoggerFactory.getLogger(StreamToBinarySizeJob.class);

    /**
     * The entry point for Flink applications.
     *
     * @param args Command line arguments
     */
    public static void main(String... args) throws Exception {
        AppConfiguration config = new AppConfiguration(args);
        log.info("config: {}", config);
        StreamToBinarySizeJob job = new StreamToBinarySizeJob(config);
        job.run();
    }

    public StreamToBinarySizeJob(AppConfiguration appConfiguration) {
        super(appConfiguration);
    }

    public void run() {
        try {
            final String jobName = getConfig().getJobName(StreamToBinarySizeJob.class.getName());
            final AppConfiguration.StreamConfig inputStreamConfig = getConfig().getStreamConfig("input");
            log.info("input stream: {}", inputStreamConfig);
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

            //Each event is a binary file so every line will have size of ingested binary file
            final DataStream<String> byteSizes = lines.flatMap((byte[] value, Collector<String> out) -> {
                                                                                        double size = (long) value.length / 1024.0;
                                                                                        out.collect(String.format("%.2f KB", size));
                                                                }).returns(Types.STRING);
            log.info("Binary File size =");
            byteSizes.printToErr();

            log.info("Executing {} job", jobName);
            env.execute(jobName);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
