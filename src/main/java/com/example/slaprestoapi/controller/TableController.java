package com.example.slaprestoapi.controller;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

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
}
