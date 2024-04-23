package com.karthik.springbulkprocessing.config;

import com.karthik.springbulkprocessing.entity.RawData;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.data.RepositoryItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;

import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;

import org.springframework.transaction.PlatformTransactionManager;
import com.karthik.springbulkprocessing.repository.RawDataRepository;

import java.io.File;

@Configuration
//@EnableBatchProcessing

public class ApplicationBatchConfig {



    @Autowired
    private RawDataRepository rawDataRepository;

    @Autowired
    private JobRepository jobRepository;

    @Autowired
    private PlatformTransactionManager transactionManager;

    public ApplicationBatchConfig(RawDataRepository rawDataRepository, JobRepository jobRepository, PlatformTransactionManager transactionManager) {
        this.rawDataRepository = rawDataRepository;
        this.jobRepository = jobRepository;
        this.transactionManager = transactionManager;
    }

    //---Iteam Reader Code Start
    @Bean
    @StepScope
    public FlatFileItemReader<RawData> itemReader(@Value("#{JobParameters[fullPathFileName]}") String pathToFile){
        FlatFileItemReader<RawData> itemReader=new FlatFileItemReader<>();
        itemReader.setResource(new FileSystemResource(new File(pathToFile)));//"C:\\karthik\\RawData.csv"
        itemReader.setLinesToSkip(1);
        itemReader.setLineMapper(lineMapper());
        return itemReader;
    }

    ////---Iteam Reader Code End

    //This linemapper method simple conver the row to JAVA object
    private LineMapper<RawData> lineMapper() {

        DefaultLineMapper<RawData> lineMapper=new DefaultLineMapper<>();
        DelimitedLineTokenizer lineTokenizer=new DelimitedLineTokenizer();
        lineTokenizer.setDelimiter(",");
        lineTokenizer.setStrict(false);
        lineTokenizer.setNames("transactionName","responseTime");

        BeanWrapperFieldSetMapper<RawData> fieldSetMapper=new BeanWrapperFieldSetMapper<>();
        fieldSetMapper.setTargetType(RawData.class);

        lineMapper.setLineTokenizer(lineTokenizer);
        lineMapper.setFieldSetMapper(fieldSetMapper);

        return lineMapper;

    }

    @Bean
    //----Item Processer
    public RawDataProcessor rawDataProcessor(){
        return new RawDataProcessor();
    }

    //----Iteam Writer
    @Bean
    public RepositoryItemWriter<RawData> itemWriter(){
        RepositoryItemWriter<RawData> itemWriter=new RepositoryItemWriter<>();
        itemWriter.setRepository(rawDataRepository);
        itemWriter.setMethodName("save");
        return itemWriter;
    }
    //-----Step
    @Bean
    public Step step(FlatFileItemReader<RawData> itemReader){
        return new StepBuilder("step-1",jobRepository)
                .<RawData,RawData>chunk(25000,transactionManager)
                .reader(itemReader)
                .processor(rawDataProcessor())
                .writer(itemWriter())
                .build();

    }

    //------Job
    @Bean
    public Job job(FlatFileItemReader<RawData> itemReader) {
        return  new JobBuilder("Rawdata-Import",jobRepository)
                .start(step(itemReader))
                .build();
    }
}
