package com.truecool.managers;

import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.Driver;

/**
 * Loads data from CSV into DB tables
 * 
 * @author Alberto Lagna
 *
 */
public class LoaderManager {

	private ConnectionManager connectionManager = new ConnectionManager();

	public LoaderManager(Driver driver, String url) throws Exception {
		connectionManager.setupConnection(driver, url);
	}

	public void loadData(String table, String filePath, boolean truncate, String dateformat) throws Exception {
		BufferedReader reader = null;

		try {
			reader = new BufferedReader(new FileReader(filePath));

			if (truncate) {
				connectionManager.performTruncate(table);
			}

			String line = null;
			while ((line = reader.readLine()) != null) {
				connectionManager.handleLine(table, line, dateformat);
			}
		} finally {
			if (reader != null) {
				reader.close();
			}
		}
	}

	


	

}
