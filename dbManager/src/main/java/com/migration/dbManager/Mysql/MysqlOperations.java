package com.migration.dbManager.Mysql;

import java.io.FileWriter;
import java.io.IOException;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import org.apache.commons.dbutils.QueryRunner;
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
		LOG.info("sourceQuery :: {}", sourceQuery);
		List<Map<String, Object>> sourceList = null;
		final QueryRunner runner = new QueryRunner();
		try (Connection conn = MysqlProperties.getSourceConnection()) {
			sourceList = runner.query(conn, sourceQuery, new MapListHandler());
		} catch (SQLException ex) {
			LOG.error("exception in getSourceListByQuery ::", ex);
		}
		return sourceList;
	}

	public List<Map<String, Object>> getDestinationListByQuery(String destinationQuery) {
		LOG.info("entered getDestinationListByQuery");
		LOG.info("destinationQuery :: {}", destinationQuery);
		List<Map<String, Object>> destinationList = null;
		final QueryRunner runner = new QueryRunner();
		try (Connection conn = MysqlProperties.getDestinationConnection()) {
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
			LOG.debug("The data is empty, unable create file: {}", filePath);
			return;
		}
		try (CSVWriter writer = new CSVWriter(new FileWriter(filePath))) {
			writer.writeNext(headers);
			String[] values = new String[headers.length];
			for (Map<String, Object> row : sourceList) {
				for (int i = 0; i < headers.length; i++) {
					Object cellValue = row.get(headers[i]);
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
			for (Map<String, Object> row : sourceList) {
				for (int i = 0; i < headers.length; i++) {
					Object cellValue = row.get(headers[i]);
					values[i] = processCell(cellValue);
				}
				writer.writeNext(values);
			}
			LOG.info("CSV file created successfully: {}", filePath);
		} catch (IOException e) {
			LOG.error("Error writing to CSV file: {}", filePath, e);
		}
	}

	public void insertData(List<Map<String, Object>> dataList, int batchSize) throws SQLException {
		final String sql = "INSERT INTO destination_table (id, column1, column2) VALUES (?, ?, ?)";
		try (final Connection conn = MysqlProperties.getDestinationConnection();
				final PreparedStatement ps = conn.prepareStatement(sql)) {
			int count = 0;
			for (Map<String, Object> data : dataList) {
				ps.setInt(1, (Integer) data.get("id"));
				ps.setString(2, (String) data.get("column1"));
				ps.setString(3, (String) data.get("column2"));
				ps.addBatch();
				if (++count % batchSize == 0) {
					ps.executeBatch();
				}
			}
			ps.executeBatch(); // insert remaining records
		}
	}

}
