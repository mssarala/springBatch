package io.spring.batch.morningstar.sequential;

import io.spring.batch.morningstar.domain.Transaction;
import io.spring.batch.morningstar.mapper.TransactionRowMapper;
import io.spring.batch.morningstar.tasklet.UnzipTasklet;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.batch.item.file.MultiResourceItemReader;
import org.springframework.batch.item.support.CompositeItemWriter;
import org.springframework.batch.item.xml.StaxEventItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.core.io.support.ResourcePatternResolver;
import org.springframework.oxm.jaxb.Jaxb2Marshaller;

import javax.sql.DataSource;
import java.io.IOException;
import java.util.Arrays;
import java.util.Date;

/**
 * @author Michael Minella
 */
@EnableBatchProcessing
@SpringBootApplication
public class SequentialStepsJobApplication {

	@Value("${fileName}")
	private String fileName;

	@Value("${directory}")
	private String processingDirectory;

	@Value("${inputXMLFiles}")
	private String filePattern;

	@Autowired
	private JobBuilderFactory jobBuilderFactory;

	@Autowired
	private StepBuilderFactory stepBuilderFactory;

	@Bean
	public Job sequentialStepsJob() {
		return this.jobBuilderFactory.get("sequentialStepsJob")
				.incrementer(new RunIdIncrementer())
				.start(unzipFile())
				.next(loadXMLToStage())
				.next(loadStageToMain())
				.build();
	}

	@Bean
	public Job xmlToStageJob() {
		return this.jobBuilderFactory.get("xmlToStageJob")
				.incrementer(new RunIdIncrementer())
				.start(loadXMLToStage())
				.next(loadStageToMain())
				.build();
	}

	@Bean
	public Job stageToMainJob() {
		return this.jobBuilderFactory.get("stageToMainJob")
				.incrementer(new RunIdIncrementer())
				.start(loadStageToMain())
				.build();
	}

	@Bean
	@StepScope
	public MultiResourceItemReader<Transaction> multiResourceItemReader() {
		Resource[] resources = null;
		ResourcePatternResolver patternResolver = new PathMatchingResourcePatternResolver();
		try {
			resources = patternResolver.getResources(filePattern);
		} catch (IOException e) {
			e.printStackTrace();
		}

		MultiResourceItemReader<Transaction> resourceItemReader = new MultiResourceItemReader<Transaction>();
		resourceItemReader.setResources(resources);
		resourceItemReader.setDelegate(xmlFileTransactionReader());
		resourceItemReader.setStrict(true);
		return resourceItemReader;
	}

	@Bean
	@StepScope
	public StaxEventItemReader<Transaction> xmlFileTransactionReader() {
		Jaxb2Marshaller unmarshaller = new Jaxb2Marshaller();
		unmarshaller.setClassesToBeBound(Transaction.class);

		StaxEventItemReader<Transaction> xmlFileTransactionReader = new StaxEventItemReader<Transaction>();
		xmlFileTransactionReader.setFragmentRootElementName("transaction");
		xmlFileTransactionReader.setUnmarshaller(unmarshaller);
		xmlFileTransactionReader.setSaveState(true);
		return xmlFileTransactionReader;
	}

	@Bean
	public JdbcBatchItemWriter<Transaction> writer(DataSource dataSource) {
		return new JdbcBatchItemWriterBuilder<Transaction>()
				.dataSource(dataSource)
				.beanMapped()
				.sql("INSERT INTO Transaction (ACCOUNT, TIMESTAMP, AMOUNT) VALUES (:account, :timestamp, :amount)")
				.build();
	}

	@Bean
	public JdbcCursorItemReader<Transaction> itemReader(DataSource dataSource) {
		return new JdbcCursorItemReaderBuilder<Transaction>()
				.dataSource(dataSource)
				.name("transactionReader")
				.sql("select ACCOUNT, TIMESTAMP, AMOUNT from TRANSACTION")
				.rowMapper(new TransactionRowMapper())
				.build();
	}

	@Bean
	public CompositeItemWriter<Transaction> compositeItemWriter(DataSource dataSource) {
		CompositeItemWriter<Transaction> compositeItemWriter = new CompositeItemWriter<>();
		compositeItemWriter.setDelegates(Arrays.asList(writerOne(dataSource), writerTwo(dataSource)));
		return compositeItemWriter;
	}


	@Bean
	public JdbcBatchItemWriter<Transaction> writerOne(DataSource dataSource) {
		return new JdbcBatchItemWriterBuilder<Transaction>()
				.dataSource(dataSource)
				.beanMapped()
				.sql("INSERT INTO TransactionOne (ACCOUNT) VALUES (:account)")
				.build();
	}

	@Bean
	public JdbcBatchItemWriter<Transaction> writerTwo(DataSource dataSource) {
		return new JdbcBatchItemWriterBuilder<Transaction>()
				.dataSource(dataSource)
				.beanMapped()
				.sql("INSERT INTO TransactionTwo (AMOUNT, TIMESTAMP) VALUES (:amount, :timestamp)")
				.build();
	}

	@Bean
	public Step unzipFile() {
		return this.stepBuilderFactory.get("unzipFile")
				.tasklet(new UnzipTasklet(fileName, processingDirectory))
				.build();
	}

	@Bean
	public Step loadXMLToStage() {
		return this.stepBuilderFactory.get("loadXMLToStage")
				.<Transaction, Transaction>chunk(100)
				.reader(multiResourceItemReader())
				.writer(writer(null))
				.build();
	}

	@Bean
	public Step loadStageToMain() {
		return this.stepBuilderFactory.get("loadStageToMain")
				.<Transaction, Transaction>chunk(100)
				.reader(itemReader(null))
				.writer(compositeItemWriter(null))
				.build();
	}

	public static void main(String[] args) {
		Long id = new Date().getTime();
		String [] newArgs = new String[] {id.toString()};
		SpringApplication.run(SequentialStepsJobApplication.class, newArgs);
	}
}

