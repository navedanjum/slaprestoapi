package com.example.slaprestoapi;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest
public class PrestoDBConnectionTest {


	static {
		try {
			Class.forName("com.facebook.presto.jdbc.PrestoDriver");
		} catch (ClassNotFoundException e) {
			throw new RuntimeException("Failed to load Presto JDBC driver", e);
		}
	}


	@Value("${spring.data.presto.jdbc.url}")
	private String dbUrl;

	@Value("${spring.data.presto.jdbc.username}")
	private String dbUsername;

	@Value("${spring.data.presto.jdbc.password}")
	private String dbPassword;

	@Test
	public void testConnection() throws Exception {
		try (Connection connection = DriverManager.getConnection(dbUrl, dbUsername, dbPassword)) {
			assertNotNull(connection, "Connection should not be null");
			String sql = "SHOW TABLES FROM hive.flex_sre_namer";
			try (Statement statement = connection.createStatement();
				 ResultSet resultSet = statement.executeQuery(sql)) {
				assertTrue(resultSet.next(), "There should be at least one table in the database");
			}
		}
	}
}
