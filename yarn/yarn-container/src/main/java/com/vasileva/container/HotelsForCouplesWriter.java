package com.vasileva.container;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.batch.item.ItemWriter;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class HotelsForCouplesWriter implements ItemWriter<String> {

    private static final int VALUES_IN_LINE = 22;
    private static final String ADULTS_COUNT = "2";
    private static final String FIELD_DELIMITER = ",";
    private static final String VALUE_DELIMITER = "#";
    private static final String EMPTY_KEY = VALUE_DELIMITER + VALUE_DELIMITER;

    private static final Log log = LogFactory.getLog(HotelsForCouplesWriter.class);

    private final long fileIdx;
    private final FileSystem fs;
    private final String outputPath;

    public HotelsForCouplesWriter(Configuration configuration, String outputPath, Long fileIdx) throws IOException {
        this.fs = FileSystem.get(configuration);
        this.outputPath = outputPath;
        this.fileIdx = fileIdx;
    }

    @Override
    public void write(List<? extends String> lines) throws IOException {
        Map<String, Long> aggregatedCounters = new HashMap<>();
        for (String line : lines) {
            if(line == null) {
                continue;
            }
            String[] values = line.split(",", -1);

            if(values.length != VALUES_IN_LINE || !ADULTS_COUNT.equals(values[11])) {
                continue;
            }

            String continent = values[18];
            String country = values[19];
            String market = values[20];

            String key = continent + VALUE_DELIMITER + country + VALUE_DELIMITER + market;

            if(EMPTY_KEY.equals(key)) {
                continue;
            }
            aggregatedCounters.put(key, aggregatedCounters.getOrDefault(key, 0L) + 1);
        }

        String filename = outputPath + fileIdx + ".csv";
        log.info("writing to: " + filename);
        try(BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fs.create(new Path(filename))))) {
            for (Map.Entry<String, Long> entry : aggregatedCounters.entrySet()) {
                String line = entry.getKey() + FIELD_DELIMITER + entry.getValue();
                log.info("writing: " + line);
                writer.write(line);
                writer.newLine();
            }
            writer.flush();
        }
    }
}
