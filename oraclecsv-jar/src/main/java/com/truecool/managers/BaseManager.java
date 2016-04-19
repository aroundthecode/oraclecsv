package com.truecool.managers;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;

import org.apache.log4j.Logger;

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
	protected Driver driver;
	protected String url;
	
	private static final Logger logger = Logger.getLogger(BaseManager.class);

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
	public BaseManager(Connection connection) throws Exception {
		this.connection = connection;
		this.connectionManager = new ConnectionManager(connection);
	}

	/**
	 * Constructor with no fields cannot be used
	 */
	protected BaseManager(){}

	/**
	 * Starts a connection to the DB (in case it wasn't provided)
	 * 
	 * @param driver
	 * @param url
	 * @throws ClassNotFoundException
	 * @throws SQLException
	 */
	protected void startConnection() throws ClassNotFoundException, SQLException {
		if (connection==null && url!=null){
			connectionManager.setupConnection(driver, url);
			connection = connectionManager.getConnection();
			// TODO replace with query
			userName = url.substring(17, url.indexOf("/"));
		}
		
	}
	
	/**
	 * Closes a connection to the DB (in case it wasn't provided)
	 */
	protected void closeConnection() {
		if ( url!=null){
			connectionManager.cleanupConnection();
		}
	}
	
	protected void logDebug(String msg){
		logger.debug(msg);
	}
	protected void logError(String msg){
		logger.error(msg);
	}
	protected void logError(String msg, Exception e){
		logger.error(msg, e);
	}


}
