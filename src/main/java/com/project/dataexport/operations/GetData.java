package com.project.dataexport.operations;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.MapListHandler;

import com.project.dataexport.connections.ConnectionManager;

public class GetData {

	public List<Map<String, Object>> fetchData() throws SQLException {
		final QueryRunner runner = new QueryRunner();
		final String query = "SELECT id, column1, column2 FROM source_table";
		try (Connection conn = ConnectionManager.getConnection()) {
			return runner.query(conn, query, new MapListHandler());
		}
	}
}
