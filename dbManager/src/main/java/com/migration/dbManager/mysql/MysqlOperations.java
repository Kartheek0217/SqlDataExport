package com.migration.dbManager.mysql;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.MapHandler;
import org.apache.commons.dbutils.handlers.MapListHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.opencsv.CSVWriter;

@Service
public class MysqlOperations {

	private static final Logger LOG = LoggerFactory.getLogger(MysqlOperations.class);

	public List<Map<String, Object>> getSourceListByQuery(String sourceQuery) {
		LOG.info("entered getSourceListByQuery");
		List<Map<String, Object>> sourceList = null;
		final QueryRunner runner = new QueryRunner();
		try (Connection conn = MysqlConnections.getSourceConnection()) {
			LOG.info("sourceQuery :: {}", sourceQuery);
			sourceList = runner.query(conn, sourceQuery, new MapListHandler());
		} catch (SQLException ex) {
			LOG.error("exception in getSourceListByQuery ::", ex);
		}
		return sourceList;
	}

	public List<Map<String, Object>> getDestinationListByQuery(String destinationQuery) {
		LOG.info("entered getDestinationListByQuery");
		List<Map<String, Object>> destinationList = null;
		final QueryRunner runner = new QueryRunner();
		try (Connection conn = MysqlConnections.getDestinationConnection()) {
			LOG.info("destinationQuery :: {}", destinationQuery);
			destinationList = runner.query(conn, destinationQuery, new MapListHandler());
		} catch (SQLException ex) {
			LOG.error("exception in getDestinationListByQuery ::", ex);
		}
		return destinationList;
	}

	private static String processCell(Object cellValue) {
		if (cellValue == null) {
			return "";
		}
		String cellData = cellValue.toString();
		if (cellData.isEmpty() || "null".equals(cellData)) {
			return "";
		}
		return cellData.replace(",", "");
	}

	public void exportSourceListToCsv(String sourceQuery, String[] headers, String filePath) {
		LOG.info("entering exportSourceListToCsv");
		List<Map<String, Object>> sourceList = getSourceListByQuery(sourceQuery);
		if (sourceList.isEmpty()) {
			LOG.debug("The data is empty, unable create the file: {}", filePath);
			return;
		}
		try (CSVWriter writer = new CSVWriter(new FileWriter(filePath))) {
			writer.writeNext(headers);
			String[] values = new String[headers.length];
			for (Map<String, Object> listRow : sourceList) {
				for (int i = 0; i < headers.length; i++) {
					Object cellValue = listRow.get(headers[i]);
					values[i] = processCell(cellValue);
				}
				writer.writeNext(values);
			}
			LOG.info("CSV file created successfully: {}", filePath);
		} catch (IOException e) {
			LOG.error("Error writing to CSV file: {}", filePath, e);
		}
	}

	public void exportDestinationListToCsv(String destinationQuery, String[] headers, String filePath) {
		LOG.info("entering exportDestinationListToCsv");
		List<Map<String, Object>> sourceList = getDestinationListByQuery(destinationQuery);
		if (sourceList.isEmpty()) {
			LOG.debug("The data is empty for file: {}", filePath);
			return;
		}
		try (CSVWriter writer = new CSVWriter(new FileWriter(filePath))) {
			writer.writeNext(headers);
			String[] values = new String[headers.length];
			for (Map<String, Object> listRow : sourceList) {
				for (int i = 0; i < headers.length; i++) {
					Object cellValue = listRow.get(headers[i]);
					values[i] = processCell(cellValue);
				}
				writer.writeNext(values);
			}
			LOG.info("CSV file created successfully: {}", filePath);
		} catch (IOException e) {
			LOG.error("Error writing to CSV file: {}", filePath, e);
		}
	}

	public void insertIntoDestinationBySourceQuery(String sourceGetQuery, String destinationInsertQuery) {
		LOG.info("Entered insertIntoDestinationBySourceQuery");
		try (Connection destinationConn = MysqlConnections.getDestinationConnection()) {
			if(destinationConn == null) {
				LOG.error("Exception in destinationConn");
				return;
			}
			int totalInserted = 0;
			List<Map<String, Object>> sourceList = getSourceListByQuery(sourceGetQuery);
			final QueryRunner runner = new QueryRunner();
			for (Map<String, Object> row : sourceList) {
				Object[] params = row.values().toArray();
				int inserted = runner.update(destinationConn, destinationInsertQuery, params);
				totalInserted += inserted;
			}
			LOG.info("Inserted {} records into destination", totalInserted);
		} catch (SQLException ex) {
			LOG.error("Exception in insertIntoDestination ::", ex);
		}
	}
	
	public void insertIntoSourceBySourceQuery(String sourceGetQuery, String sourceInsert) {
		LOG.info("Entered insertIntoDestinationBySourceQuery");
		try (Connection sourceConn = MysqlConnections.getSourceConnection()) {
			int totalInserted = 0;
			List<Map<String, Object>> sourceList = getSourceListByQuery(sourceGetQuery);
			final QueryRunner runner = new QueryRunner();
			for (Map<String, Object> row : sourceList) {
				Object[] params = row.values().toArray();
				int inserted = runner.update(sourceConn, sourceInsert, params);
				totalInserted += inserted;
			}
			LOG.info("Inserted {} records into destination", totalInserted);
		} catch (SQLException ex) {
			LOG.error("Exception in insertIntoDestination ::", ex);
		}
	}

	public Map<String, Object> insertAndGetGeneratedKeys(String sql, String[] inputDataList) throws Exception {
		Map<String, Object> generatedKeys = new HashMap<>();
		try (Connection conn = MysqlConnections.getDestinationConnection()) {
			if (conn == null) {
				throw new SQLException("Failed to obtain database connection");
			}
			QueryRunner runner = new QueryRunner();
			conn.setAutoCommit(false);
			try {
				// Execute the insert and retrieve generated keys
				generatedKeys = runner.insert(conn, sql, new MapHandler(), (Object[]) inputDataList);
				conn.commit();
			} catch (Exception e) {
				conn.rollback();
				LOG.error("Error executing insert statement", e);
				throw e;
			} finally {
				conn.setAutoCommit(true);
			}
		}

		return generatedKeys;
	}

}
