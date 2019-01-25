package com.vasileva.container.appmaster;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParametersIncrementer;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.partition.PartitionHandler;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.core.partition.support.SimplePartitioner;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.hadoop.store.split.Splitter;
import org.springframework.data.hadoop.store.split.StaticLengthSplitter;
import org.springframework.yarn.batch.BatchSystemConstants;
import org.springframework.yarn.batch.config.EnableYarnBatchProcessing;
import org.springframework.yarn.batch.partition.SplitterPartitionHandler;
import org.springframework.yarn.batch.partition.SplitterPartitioner;
import org.springframework.yarn.batch.partition.StaticPartitionHandler;


@Configuration
@EnableAutoConfiguration
@EnableYarnBatchProcessing
public class AppmasterApp {
    @Autowired
    private JobBuilderFactory jobFactory;

    @Autowired
    private StepBuilderFactory stepFactory;

    @Bean
    public Job job() throws Exception {
        return jobFactory.get("job")
                .incrementer(jobParametersIncrementer())
                .start(map())
                .next(reduce())
                .build();
    }

    @Bean
    public JobParametersIncrementer jobParametersIncrementer() {
        return new RunIdIncrementer();
    }

    @Bean
    protected Step map() throws Exception {
        return stepFactory
                .get("map")
                .partitioner("remoteStep", partitioner(null))
                .partitionHandler(partitionHandler())
                .build();
    }

    @Bean
    @StepScope
    protected Partitioner partitioner(@Value(BatchSystemConstants.JP_SPEL_KEY_INPUTPATTERNS) String inputPatterns) {

        SplitterPartitioner partitioner = new SplitterPartitioner();
        partitioner.setSplitter(splitter());
        partitioner.setInputPatterns(inputPatterns);
        return partitioner;
    }

    @Bean
    protected Splitter splitter() {
        return new StaticLengthSplitter(1000);

    }

    @Bean
    protected PartitionHandler partitionHandler() {
        SplitterPartitionHandler handler = new SplitterPartitionHandler();
        handler.setStepName("remoteStep");
        return handler;
    }

    @Bean
    protected Step reduce() throws Exception {
        return stepFactory
                .get("reduce")
                .partitioner("remoteStep1", simplePartitioner())
                .partitionHandler(singlePartitionHandler())
                .build();
    }

    @Bean
    protected Partitioner simplePartitioner() {
        return new SimplePartitioner();
    }

    @Bean
    protected PartitionHandler singlePartitionHandler() {
        StaticPartitionHandler handler = new StaticPartitionHandler();
        handler.setStepName("remoteStep1");
        handler.setGridSize(1);
        return handler;
    }

    public static void main(String[] args) {
        SpringApplication.run(AppmasterApp.class, args);
    }
}
