package com.project.dataexport;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import com.project.dataexport.connections.ConnectionManager;
import com.project.dataexport.operations.GetData;
import com.project.dataexport.operations.InsertData;

@SpringBootApplication
public class DataexportApplication {

	public static void main(String[] args) {
		SpringApplication.run(DataexportApplication.class, args);
		start();
	}

	public static void start() {
		try {
			final GetData dataFetcher = new GetData();
			final List<Map<String, Object>> dataList = dataFetcher.fetchData();
			final InsertData dataInserter = new InsertData();
			final int batchSize = 1000; // optimal batch size
			dataInserter.insertData(dataList, batchSize);
			System.out.println("Bulk data transfer completed successfully.");
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			ConnectionManager.closeDataSource();
			System.out.println("Data sources closed.");
		}
	}
}
