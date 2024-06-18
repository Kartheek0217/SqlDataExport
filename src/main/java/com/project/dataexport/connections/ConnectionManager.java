package com.project.dataexport.connections;

import java.sql.Connection;
import java.sql.SQLException;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

public class ConnectionManager {

	private static final HikariDataSource dataSource;

	static {
		final HikariConfig config = new HikariConfig();
		config.setJdbcUrl("jdbc:mysql://localhost:3306/mydb");
		config.setUsername("root");
		config.setPassword("password");
		config.setMaximumPoolSize(10);
		config.setMinimumIdle(5);
		config.setIdleTimeout(30000);
		config.setMaxLifetime(60000);
		config.setConnectionTimeout(30000);

		dataSource = new HikariDataSource(config);
	}

	public static Connection getConnection() throws SQLException {
		return dataSource.getConnection();
	}

	public static void closeDataSource() {
		if (dataSource != null && !dataSource.isClosed()) {
			dataSource.close();
		}
	}
}
