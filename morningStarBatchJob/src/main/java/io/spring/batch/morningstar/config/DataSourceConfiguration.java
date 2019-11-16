package io.spring.batch.morningstar.config;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import javax.sql.DataSource;

@Configuration
public class DataSourceConfiguration {

    @Autowired
    private Environment env;

    @Bean("mainDataSource")
    public DataSource domainDataSource() {
        return DataSourceBuilder.create()
                .url(env.getProperty("main.datasource.url"))
                .driverClassName(env.getProperty("main.datasource.driverClassName"))
                .username(env.getProperty("main.datasource.username"))
                .password(env.getProperty("main.datasource.password"))
                .build();
    }

    @Bean("batchDataSource")
    @Primary
    public DataSource batchDataSource() {
        return DataSourceBuilder.create()
                .url(env.getProperty("batch.datasource.url"))
                .driverClassName(env.getProperty("batch.datasource.driverClassName"))
                .username(env.getProperty("batch.datasource.username"))
                .password(env.getProperty("batch.datasource.password"))
                .build();
    }
}