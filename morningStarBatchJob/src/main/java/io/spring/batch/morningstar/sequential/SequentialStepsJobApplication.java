package io.spring.batch.morningstar.sequential;

import io.spring.batch.morningstar.domain.Transaction;
import io.spring.batch.morningstar.domain.Transactionxml;
import io.spring.batch.morningstar.mapper.TransactionXmlMapper;
import io.spring.batch.morningstar.processor.XMLItemProcessor;
import io.spring.batch.morningstar.tasklet.UnzipTasklet;
import io.spring.batch.morningstar.writer.MainTableWriter;
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
	public JdbcBatchItemWriter<Transactionxml> writer(DataSource dataSource) {
		return new JdbcBatchItemWriterBuilder<Transactionxml>()
				.dataSource(dataSource)
				.beanMapped()
				.sql("INSERT INTO TRANSACTIONXML (ISIN, RAWDATA) VALUES (:isin, :rawData)")
				.build();
	}

	@Bean
	public JdbcCursorItemReader<Transactionxml> itemReader(DataSource dataSource) {
		return new JdbcCursorItemReaderBuilder<Transactionxml>()
				.dataSource(dataSource)
				.name("transactionReader")
				.sql("select ISIN, RAWDATA from TRANSACTIONXML")
				.rowMapper(new TransactionXmlMapper())
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
				.<Transaction, Transactionxml>chunk(100)
				.reader(multiResourceItemReader())
				.processor(new XMLItemProcessor())
				.writer(writer(null))
				.build();
	}

	@Bean
	public Step loadStageToMain() {
		return this.stepBuilderFactory.get("loadStageToMain")
				.<Transactionxml, Transactionxml>chunk(100)
				.reader(itemReader(null))
				.writer(new MainTableWriter())
				.build();
	}

	public static void main(String[] args) {
		Long id = new Date().getTime();
		String [] newArgs = new String[] {id.toString()};
		SpringApplication.run(SequentialStepsJobApplication.class, newArgs);
	}
}

