package com.project.dataexport.operations;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import com.project.dataexport.connections.ConnectionManager;

public class InsertData {

	public void insertData(List<Map<String, Object>> dataList, int batchSize) throws SQLException {
		
		final String sql = "INSERT INTO destination_table (id, column1, column2) VALUES (?, ?, ?)";
		try (final Connection conn = ConnectionManager.getConnection();
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
