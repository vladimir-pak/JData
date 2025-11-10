package com.gpb.jdata.config;

import com.zaxxer.hikari.HikariDataSource;
import com.gpb.jdata.utils.pconnection.DBConnector;
import com.gpb.jdata.utils.pconnection.PostgreSQLConnector;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.orm.jpa.LocalContainerEntityManagerFactoryBean;
import org.springframework.orm.jpa.vendor.HibernateJpaVendorAdapter;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import javax.sql.DataSource;
import java.util.Properties;

@Configuration
@EnableTransactionManagement
@ComponentScan(basePackages = {
        "gpb.demogpb",
        "gpb.demogpb.repository"
})
public class AppConfig {
    @Bean
    public DataSource dataSource() {
        HikariDataSource dataSource = new HikariDataSource();
        dataSource.setDriverClassName("org.postgresql.Driver");
        dataSource.setJdbcUrl("jdbc:postgresql://ord-d12asl:5432/jdata_db");
        dataSource.setUsername("jdata_user");
        dataSource.setPassword("jdatapass");
        return dataSource;
    }

    @Bean
    public LocalContainerEntityManagerFactoryBean entityManagerFactory(DataSource dataSource) {
        LocalContainerEntityManagerFactoryBean em = new LocalContainerEntityManagerFactoryBean();
        em.setDataSource(dataSource);
        em.setPackagesToScan(
                "gpb.demogpb.entity.master",
                "gpb.demogpb.entity.replication"
        );
        HibernateJpaVendorAdapter vendorAdapter = new HibernateJpaVendorAdapter();
        em.setJpaVendorAdapter(vendorAdapter);
        Properties properties = new Properties();
        properties.setProperty("hibernate.dialect", "org.hibernate.dialect.PostgreSQLDialect");
        properties.setProperty("hibernate.hbm2ddl.auto", "none");
        em.setJpaProperties(properties);
        return em;
    }

    private Properties hibernateProperties() {
        Properties properties = new Properties();
        properties.setProperty("hibernate.dialect", "org.hibernate.dialect.PostgreSQLDialect");
        properties.setProperty("hibernate.hbm2ddl.auto", "none");
        return properties;
    }

    @Bean
    public PostgreSQLConnector postgreSQLConnector() {
        return new PostgreSQLConnector();
    }
    
    @Bean
    public DBConnector dbConnector() {
        return new DBConnector();
    }
}
