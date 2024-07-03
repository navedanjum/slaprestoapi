package com.example.slaprestoapi.controller;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Controller
public class TableController {

	@Value("${spring.data.presto.jdbc.url}")
	private String dbUrl;

	@Value("${spring.data.presto.jdbc.username}")
	private String dbUsername;

	@Value("${spring.data.presto.jdbc.password}")
	private String dbPassword;

	@GetMapping("/tables")
	public String getTables(Model model) {
		List<String> tables = new ArrayList<>();

		try (Connection connection = DriverManager.getConnection(dbUrl, dbUsername, dbPassword);
			 Statement statement = connection.createStatement();

			 ResultSet resultSet = statement.executeQuery("SHOW TABLES FROM hive.flex_sre_namer")) {

			while (resultSet.next()) {
				tables.add(resultSet.getString(1));
			}

		} catch (Exception e) {
			e.printStackTrace();
		}

		model.addAttribute("tables", tables);
		return "tables";
	}

//	@GetMapping("/records")
//	public String getRecords(Model model,
//							 @RequestParam String action,
//							 @RequestParam String eventDate) {
//		List<String> records = new ArrayList<>();
//
//		String query = String.format(
//				"SELECT * FROM flex_sre_namer.flex_customer_journey_sla sla " +
//						"WHERE sla.action = '%s' AND sla.event_date > '%s' " +
//						"ORDER BY sla.event_date DESC LIMIT 10", action, eventDate);
//
//		try (Connection connection = DriverManager.getConnection(dbUrl, dbUsername, dbPassword);
//			 Statement statement = connection.createStatement();
//			 ResultSet resultSet = statement.executeQuery(query)) {
//
//			while (resultSet.next()) {
//				// Assuming the table has a column named "data"
//				records.add(resultSet.getString("data"));
//			}
//
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//
//		model.addAttribute("records", records);
//		return "records";
//	}

	@GetMapping("/records")
	public String getRecords(Model model,
							 @RequestParam String action,
							 @RequestParam String eventDate) {
		List<Map<String, Object>> records = new ArrayList<>();
		List<String> columns = new ArrayList<>();

		String query = "SELECT * FROM flex_sre_namer.flex_customer_journey_sla sla " +
				"WHERE sla.action = ? AND sla.event_date > ? " +
				"ORDER BY sla.event_date DESC LIMIT 10";

		try (Connection connection = DriverManager.getConnection(dbUrl, dbUsername, dbPassword);
			 PreparedStatement preparedStatement = connection.prepareStatement(query)) {

			preparedStatement.setString(1, action);
			preparedStatement.setString(2, eventDate);

			try (ResultSet resultSet = preparedStatement.executeQuery()) {
				ResultSetMetaData metaData = resultSet.getMetaData();
				int columnCount = metaData.getColumnCount();

				for (int i = 1; i <= columnCount; i++) {
					columns.add(metaData.getColumnName(i));
				}

				while (resultSet.next()) {
					Map<String, Object> record = new HashMap<>();
					for (String column : columns) {
						record.put(column, resultSet.getObject(column));
					}
					records.add(record);
				}
			}

			// Log the retrieved records for debugging
			System.out.println("Retrieved columns: " + columns);
			System.out.println("Retrieved records: " + records);

		} catch (Exception e) {
			e.printStackTrace();
		}

		model.addAttribute("columns", columns);
		model.addAttribute("records", records);
		return "records";
	}
}
