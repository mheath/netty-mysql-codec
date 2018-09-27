package com.github.mheath.netty.codec.mysql.integration.server;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLWarning;
import java.sql.Statement;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 *
 */
public class JdbcClientTest {

	private static TestServer server;
	private static final int port = Integer.valueOf(System.getProperty("server.port", "37294"));

	@BeforeAll
	public static void startServer() {
		server = new TestServer(port);
	}

	@AfterAll
	public static void stopServer() {
		server.close();
	}

	@Test
	@Disabled
	public void connectAndQuery() throws Exception {
		try (Connection conn = DriverManager.getConnection("jdbc:mysql://localhost:" + server.getPort() + "/test", server.getUser(), server.getPassword())) {
			Assertions.assertThat(conn.isClosed()).isFalse();
			final SQLWarning warnings = conn.getWarnings();
			try (Statement statement = conn.createStatement()) {
				try (ResultSet rs = statement.executeQuery("SELECT 1")) {
					while (rs.next()) {
						System.out.println(rs.getString(1));
					}
				}
			}
		}
	}







}
