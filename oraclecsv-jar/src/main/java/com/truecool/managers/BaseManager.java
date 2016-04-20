package com.truecool.managers;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;

import javax.sql.DataSource;

/**
 * Common parts of the export and load managers.
 * Can be both used passing driver and url or directly a connection.
 * 
 * @author Alberto Lagna
 *
 */
public abstract class BaseManager {

	protected ConnectionManager connectionManager;
	protected Connection connection;
	protected String userName;
	private Driver driver;
	private String url;
	private DataSource dataSource; 
	
	/**
	 * Constructor with driver and url, in case a connection is not provided.
	 * @param driver
	 * @param url
	 * @throws Exception
	 */
	public BaseManager(Driver driver, String url) throws Exception {
		this.connectionManager = new ConnectionManager();
		this.driver=driver;
		this.url=url;
	}
	
	/**
	 * Constructor in case a provided connection is used.
	 * @param connection
	 * @throws Exception
	 */
	public BaseManager(DataSource datasource) throws Exception {
		this.connectionManager = new ConnectionManager();
		this.dataSource = datasource;
	}

	/**
	 * Constructor with no fields cannot be used
	 */
	protected BaseManager(){}

	/**
	 * Starts a connection to the DB
	 * 
	 * @param driver
	 * @param url
	 * @throws ClassNotFoundException
	 * @throws SQLException
	 */
	protected void startConnection() throws ClassNotFoundException, SQLException {
		if (url!=null && driver!=null){
			connectionManager.setupConnection(driver, url);
			connection = connectionManager.getConnection();
			// TODO replace with query (used for test purposes)
			userName = url.substring(17, url.indexOf("/"));
		} else if(dataSource!=null) {
			dataSource.getConnection();
		} else 
			logError("Not enough parameters to start a connection");
		
	}
	
	/**
	 * Closes a connection to the DB
	 */
	protected void closeConnection() {
		connectionManager.cleanupConnection();
	}
	
	protected void logDebug(String msg){
		System.out.println(msg);
	}
	protected void logError(String msg){
		System.err.println(msg);
	}
	protected void logError(String msg, Exception e){
		System.err.println(msg);
	}


}
