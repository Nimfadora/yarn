package com.vasileva.container;

import org.apache.hadoop.fs.Path;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.hadoop.store.DataStoreReader;
import org.springframework.data.hadoop.store.input.TextFileReader;
import org.springframework.data.hadoop.store.split.GenericSplit;
import org.springframework.data.hadoop.store.split.Split;
import org.springframework.yarn.batch.config.EnableYarnRemoteBatchProcessing;
import org.springframework.yarn.batch.item.DataStoreItemReader;
import org.springframework.yarn.batch.item.PassThroughLineDataMapper;

import java.io.IOException;

import static org.springframework.yarn.batch.BatchSystemConstants.*;

@Configuration
@EnableAutoConfiguration
@EnableYarnRemoteBatchProcessing
public class ContainerApp {

    @Autowired
    private StepBuilderFactory stepBuilder;

    @Autowired
    private org.apache.hadoop.conf.Configuration configuration;

    @Bean
    protected Step remoteStep() throws IOException {
        return stepBuilder
                .get("remoteStep")
                .<String, String>chunk(1000)
                .reader(itemReader(null, null, null))
                .writer(itemWriter(null, null))
                .build();

    }

    @Bean
    @StepScope
    protected DataStoreItemReader<String> itemReader(@Value(SEC_SPEL_KEY_FILENAME) String fileName,
                                                     @Value(SEC_SPEL_KEY_SPLITSTART) Long splitStart,
                                                     @Value(SEC_SPEL_KEY_SPLITLENGTH) Long splitLength) {
        Split split = new GenericSplit(splitStart, splitLength, null);
        DataStoreReader<String> reader = new TextFileReader(configuration, new Path(fileName), null, split, null);
        DataStoreItemReader<String> itemReader = new DataStoreItemReader<>();
        itemReader.setDataStoreReader(reader);
        itemReader.setLineDataMapper(new PassThroughLineDataMapper());

        return itemReader;
    }

    @Bean
    @StepScope
    protected ItemWriter<String> itemWriter(@Value("#{jobParameters['outputPatterns']}") String fileName,
                                            @Value(SEC_SPEL_KEY_SPLITSTART) Long splitStart) throws IOException {
        return new HotelsForCouplesWriter(configuration, fileName, splitStart);
    }

    @Bean
    protected Step remoteStep1() throws IOException {
        return stepBuilder
                .get("remoteStep1")
                .tasklet(tasklet(null, null))
                .build();
    }

    @Bean
    @StepScope
    protected Tasklet tasklet(@Value("#{jobParameters['outputPatterns']}") String input,
                              @Value("#{jobParameters['resultFilename']}") String output) throws IOException {
        return new TopHotelsForCouplesTasklet(configuration, input, output);
    }

    public static void main(String[] args) {
        SpringApplication.run(ContainerApp.class, args);
    }
}
