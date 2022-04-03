/**
 * Copyright @ 2022科大讯飞。 All rights reserved.
 *
 * @author: gdliu3
 * @Date: 2022/4/3 21:44
 */
package hive.config;

import com.alibaba.druid.pool.DruidDataSource;
import lombok.Data;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;

/**
 * Copyright @ 2022科大讯飞。 All rights reserved.
 * @Title HiveDruidConfig
 * @Project geek
 * @Description TODO
 * @author gdliu3
 * @date 2022/4/3 21:44
 */
@Configuration
@Data
@ConfigurationProperties(prefix = "hive")
public class HiveDruidConfig {
		private String url;
		private String user;
		private String password;
		private String driverClassName;
		private int initialSize;
		private int minIdle;
		private int maxActive;
		private int maxWait;
		private int timeBetweenEvictionRunsMillis;
		private int minEvictableIdleTimeMillis;
		private String validationQuery;
		private boolean testWhileIdle;
		private boolean testOnBorrow;
		private boolean testOnReturn;
		private boolean poolPreparedStatements;
		private int maxPoolPreparedStatementPerConnectionSize;
		private int connectionErrorRetryAttempts;
		private boolean breakAfterAcquireFailure;

		@Bean(name = "hiveDruidDataSource")
		@Qualifier("hiveDruidDataSource")
		public DataSource dataSource() {
			DruidDataSource datasource = new DruidDataSource();
			datasource.setUrl(url);
			datasource.setUsername(user);
			datasource.setPassword(password);
			datasource.setDriverClassName(driverClassName);
			datasource.setInitialSize(initialSize);
			datasource.setMinIdle(minIdle);
			datasource.setMaxActive(maxActive);
			datasource.setMaxWait(maxWait);
			datasource.setTimeBetweenEvictionRunsMillis(timeBetweenEvictionRunsMillis);
			datasource.setMinEvictableIdleTimeMillis(minEvictableIdleTimeMillis);
			datasource.setValidationQuery(validationQuery);
			datasource.setTestWhileIdle(testWhileIdle);
			datasource.setTestOnBorrow(testOnBorrow);
			datasource.setTestOnReturn(testOnReturn);
			datasource.setPoolPreparedStatements(poolPreparedStatements);
			datasource.setMaxPoolPreparedStatementPerConnectionSize(maxPoolPreparedStatementPerConnectionSize);
			datasource.setConnectionErrorRetryAttempts(connectionErrorRetryAttempts);
			datasource.setBreakAfterAcquireFailure(breakAfterAcquireFailure);
			return datasource;
		}

		@Bean(name = "hiveDruidTemplate")
		public JdbcTemplate hiveDruidTemplate(@Qualifier("hiveDruidDataSource") DataSource dataSource) {
			return new JdbcTemplate(dataSource);
		}
}
