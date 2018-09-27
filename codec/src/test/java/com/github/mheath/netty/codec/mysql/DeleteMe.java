package com.github.mheath.netty.codec.mysql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.regex.Pattern;

/**
 *
 */
public class DeleteMe {

	private static Pattern SETTINGS_PATTERN = Pattern.compile("@@(\\w+)\\sAS\\s(\\w+)");

	public static void main(String[] args) throws Exception {
		Connection conn = DriverManager.getConnection("jdbc:mysql://localhost?useSSL=false", "root", "");
		conn.close();
	}
}
