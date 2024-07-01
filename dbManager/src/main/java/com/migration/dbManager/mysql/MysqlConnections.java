package com.migration.dbManager.mysql;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

public class MysqlConnections {

	private static final Logger LOG = LoggerFactory.getLogger(MysqlConnections.class);

	private static final HikariDataSource sourceDataSource;
	private static final HikariDataSource destinationDataSource;

	static {
		final Properties props = loadProperties();
		sourceDataSource = createDataSource(props, "source");
		destinationDataSource = createDataSource(props, "destination");
	}

	private static Properties loadProperties() {
		final Properties props = new Properties();
		try (InputStream input = Thread.currentThread().getContextClassLoader()
				.getResourceAsStream("config/mysql.properties")) {
			if (input == null) {
				LOG.error("Unable to find connection.properties");
			}
			props.load(input);
		} catch (IOException ex) {
			LOG.error("Error loading connection.properties", ex);
		}
		return props;
	}

	private static HikariDataSource createDataSource(Properties props, String prefix) {
		HikariConfig config = new HikariConfig();
		config.setJdbcUrl(props.getProperty(prefix + ".jdbcUrl"));
		config.setUsername(props.getProperty(prefix + ".username"));
		config.setPassword(props.getProperty(prefix + ".password"));
		config.setMaximumPoolSize(Integer.parseInt(props.getProperty(prefix + ".maximumPoolSize")));
		config.setMinimumIdle(Integer.parseInt(props.getProperty(prefix + ".minimumIdle")));
		config.setMaxLifetime(Long.parseLong(props.getProperty(prefix + ".maxLifetime")));
		config.setConnectionTimeout(Long.parseLong(props.getProperty(prefix + ".connectionTimeout")));
		config.addDataSourceProperty("useSSL", props.getProperty(prefix + ".useSSL"));
		config.addDataSourceProperty("allowPublicKeyRetrieval", props.getProperty(prefix + ".allowPublicKeyRetrieval"));
		config.addDataSourceProperty("serverTimezone", props.getProperty(prefix + ".serverTimezone"));
		return new HikariDataSource(config);
	}

	public static Connection getSourceConnection() {
		Connection connection = null;
		LOG.info("entered sourceConnection");
		try {
			connection = sourceDataSource.getConnection();
		} catch (SQLException ex) {
			connection = null;
			LOG.error("unable to connect with sourceConnection db", ex);
		}
		return connection;
	}

	public static Connection getDestinationConnection() {
		Connection connection = null;
		LOG.info("entered sourceConnection");
		try {
			connection = destinationDataSource.getConnection();
		} catch (SQLException ex) {
			connection = null;
			LOG.error("unable to connect with sourceConnection db", ex);
		}
		return connection;
	}
}
