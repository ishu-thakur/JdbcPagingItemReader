package com.springbatchexample.config;

import com.springbatchexample.component.LoanItemProcessor;
import com.springbatchexample.component.LoanResultRowMapper;
import com.springbatchexample.entity.Loan;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.builder.FlowJobBuilder;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.step.builder.SimpleStepBuilder;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.support.PostgresPagingQueryProvider;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;

import javax.sql.DataSource;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

@EnableBatchProcessing
@Configuration
public class SpringBatchConfig {


    @Autowired
    private DataSource dataSource;

    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    @Bean
    public JdbcPagingItemReader<Loan> jdbcPagingItemReader() {
        JdbcPagingItemReader<Loan> pagingItemReader = new JdbcPagingItemReader<>();

        pagingItemReader.setDataSource(dataSource);
        pagingItemReader.setFetchSize(20);
        pagingItemReader.setRowMapper(new LoanResultRowMapper());
        pagingItemReader.setPageSize(1);

        PostgresPagingQueryProvider postgresPagingQueryProvider = new PostgresPagingQueryProvider();
        postgresPagingQueryProvider.setSelectClause("id, contact_id, created_at");
        postgresPagingQueryProvider.setFromClause("from Loan");
        postgresPagingQueryProvider.setWhereClause("where status IN ('Active - Bad Standing', 'Active - Good Standing', 'Active - Marked for Closure', 'Active - Matured', 'Active - Recovery Plan')");

        Map<String, Order> orderById = new HashMap<>();
        orderById.put("id", Order.ASCENDING);
        postgresPagingQueryProvider.setSortKeys(orderById);
        pagingItemReader.setQueryProvider(postgresPagingQueryProvider);

        return pagingItemReader;
    }

    @Bean
    public FlatFileItemWriter<Loan> writer() {
        FlatFileItemWriter<Loan> writer = new FlatFileItemWriter<>();
        writer.setResource(new FileSystemResource("C://Users/ishu.thakur/Desktop/JdbcPagingItemReader-spring-batch-example/src/main/resources/data.csv"));
        writer.setLineAggregator(getDelimitedLineAggregator());
        return writer;
    }

    private DelimitedLineAggregator<Loan> getDelimitedLineAggregator() {
        BeanWrapperFieldExtractor<Loan> beanWrapperFieldExtractor = new BeanWrapperFieldExtractor<Loan>();
        beanWrapperFieldExtractor.setNames(new String[]{"id", "contactId", "createdAt"});

        DelimitedLineAggregator<Loan> aggregator = new DelimitedLineAggregator<Loan>();
        aggregator.setDelimiter(",");
        aggregator.setFieldExtractor(beanWrapperFieldExtractor);
        return aggregator;

    }

    @Bean
    public Step getDbToCsvStep() {
        StepBuilder stepBuilder = stepBuilderFactory.get("getDbToCsvStep" + Instant.now());
        SimpleStepBuilder<Loan, Loan> simpleStepBuilder = stepBuilder.chunk(1);
        return simpleStepBuilder.reader(jdbcPagingItemReader()).processor(processor()).writer(writer()).build();
    }

    @Bean
    public Job dbToCsvJob() {
        JobBuilder jobBuilder = jobBuilderFactory.get("dbToCsvJob" + Instant.now());
        jobBuilder.incrementer(new RunIdIncrementer());
        FlowJobBuilder flowJobBuilder = jobBuilder.flow(getDbToCsvStep()).end();
        Job job = flowJobBuilder.build();
        return job;
    }

    @Bean
    public LoanItemProcessor processor() {
        return new LoanItemProcessor();
    }

}
