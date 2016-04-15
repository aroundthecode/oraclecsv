package com.truecool.managers;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;

/**
 * Common parts of the export and load managers
 * 
 * @author Alberto Lagna
 *
 */
public abstract class BaseManager {

	protected ConnectionManager connectionManager = new ConnectionManager();
	protected Connection connection;
	protected String userName;
	protected Driver driver;
	protected String url;
	

	/**
	 * Constructor with the mandatory fields
	 * @param driver
	 * @param url
	 * @throws Exception
	 */
	public BaseManager(Driver driver, String url) throws Exception {
		this.driver=driver;
		this.url=url;
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
		connectionManager.setupConnection(driver, url);
		connection = connectionManager.getConnection();
		
		// TODO oracle specific
		userName = url.substring(17, url.indexOf("/"));
	}
	
	
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
//		e.printStackTrace();
	}


}
