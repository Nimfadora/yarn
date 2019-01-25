package com.vasileva.container;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;

import java.io.*;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class TopHotelsForCouplesTasklet implements Tasklet {

    private static final int HOTELS_LIMIT = 3;

    private static final Log log = LogFactory.getLog(TopHotelsForCouplesTasklet.class);

    private final FileSystem fs;
    private final String inputPattern;
    private final String outputFile;

    public TopHotelsForCouplesTasklet(Configuration configuration, String inputPattern, String outputFile) throws IOException {
        this.fs = FileSystem.get(configuration);
        this.inputPattern = inputPattern;
        this.outputFile = outputFile;
    }

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        Map<String, Long> hotel2count = new HashMap<>();
        try(BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(new Path(inputPattern + "*"))))) {
            String line = reader.readLine();
            while (line != null) {
                String[] key2count = line.split(",", 2);
                Long count = Long.parseLong(key2count[1]);
                hotel2count.put(key2count[0], hotel2count.getOrDefault(key2count[0], 0L) + count);
                line = reader.readLine();
            }
        }
        log.info("reducing results");

        List<String> topHotels = hotel2count.entrySet().stream()
                .sorted(Comparator.comparingLong(Map.Entry::getValue))
                .limit(HOTELS_LIMIT)
                .map(e -> e.getKey() + " : " +  e.getValue())
                .collect(Collectors.toList());

        try(BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fs.create(new Path(outputFile))))) {
            for (String hotel : topHotels) {
                log.info("top hotel: " + hotel);
                writer.write(hotel);
                writer.newLine();
            }
            writer.flush();
        }
        return RepeatStatus.FINISHED;
    }
}
