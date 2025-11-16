package com.gpb.jdata.config;

import java.util.Properties;

import javax.sql.DataSource;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.orm.jpa.LocalContainerEntityManagerFactoryBean;
import org.springframework.orm.jpa.vendor.HibernateJpaVendorAdapter;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import com.gpb.jdata.properties.JdataDbProperties;
import com.zaxxer.hikari.HikariDataSource;

import jakarta.persistence.EntityManagerFactory;
import lombok.RequiredArgsConstructor;

@Configuration
@EnableTransactionManagement
@ComponentScan(basePackages = {
        "com.gpb.jdata",
        "com.gpb.jdata.repository",
        "com.gpb.jdata.orda.repository"
})
@RequiredArgsConstructor
public class AppConfig {

    private final JdataDbProperties dbProperties;

    @Bean
    @Primary
    public DataSource dataSource() {
        HikariDataSource dataSource = new HikariDataSource();
        dataSource.setDriverClassName("org.postgresql.Driver");
        dataSource.setJdbcUrl(dbProperties.getUrl());
        dataSource.setUsername(dbProperties.getUsername());
        dataSource.setPassword(dbProperties.getPassword());
        return dataSource;
    }

    @Bean
    public LocalContainerEntityManagerFactoryBean entityManagerFactory(DataSource dataSource) {
        LocalContainerEntityManagerFactoryBean em = new LocalContainerEntityManagerFactoryBean();
        em.setDataSource(dataSource);
        em.setPackagesToScan(
                "com.gpb.jdata.models.master",
                "com.gpb.jdata.models.replication",
                "com.gpb.jdata.orda.model"
        );
        HibernateJpaVendorAdapter vendorAdapter = new HibernateJpaVendorAdapter();
        em.setJpaVendorAdapter(vendorAdapter);
        Properties properties = new Properties();
        properties.setProperty("hibernate.dialect", "org.hibernate.dialect.PostgreSQLDialect");
        properties.setProperty("hibernate.hbm2ddl.auto", "none");
        em.setJpaProperties(properties);
        return em;
    }

    @Bean
    public PlatformTransactionManager transactionManager(EntityManagerFactory entityManagerFactory) {
        if (entityManagerFactory != null) {
            return new JpaTransactionManager(entityManagerFactory);
        }
        return new JpaTransactionManager();
    }

    @Bean(name = "jdataDataSource")
    @Primary
    public JdbcTemplate jdbcTemplate(DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }
}
