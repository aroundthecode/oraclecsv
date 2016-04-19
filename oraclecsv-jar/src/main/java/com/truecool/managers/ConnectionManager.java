package com.truecool.managers;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.StringTokenizer;

//import oracle.jdbc.OraclePreparedStatement;



import org.apache.tools.ant.BuildException;

import com.truecool.utils.StringUtils;

/**
 * Manages the connection to the DB and uses it.
 * 
 * @author Alberto Lagna
 *
 */
public class ConnectionManager {

	protected Connection connection = null;
	private String[] colTypes = null;

	/**
	 * Constructor used in case the connection is not provided
	 */
	public ConnectionManager(){	}
	
	/**
	 * Constructor used in case the connection is provided
	 * 
	 * @param connection
	 */
	public ConnectionManager(Connection connection){
		this.connection=connection;
	}
	
	public void setupConnection(Driver driver, String url) throws ClassNotFoundException, SQLException {
		DriverManager.registerDriver(driver);
		connection = DriverManager.getConnection(url);
	}

	public void cleanupConnection() {
		
		if (getConnection() != null) {
			try {
				if( getConnection().getAutoCommit() == false){
					getConnection().commit();
				}
				// FIXME understand why it fails
//				getConnection().close();
			} catch (SQLException e) {
				throw new BuildException(e);
			}
		}
	}

	public Connection getConnection() {
		return connection;
	}

	private String prepareSql( String table, String line) throws Exception {
		PreparedStatement statement = null;
		ResultSetMetaData metaData = null;
		String metaDataSql = "SELECT * FROM " + table + " WHERE 1 = 2";

		statement = getConnection().prepareStatement(metaDataSql);
		statement.execute();
		metaData = statement.getMetaData();


		String sql = "INSERT INTO " + table + "(";
		
		int size = metaData.getColumnCount();
		colTypes = new String[size];
		
		for (int index = 1; index <= size; index ++) {
			String colName = metaData.getColumnName(index);
			colTypes[index-1] = metaData.getColumnClassName(index);
			
			sql += colName;

			if (index < metaData.getColumnCount()) {
				sql += ",";
			}
		}
//		System.out.println(Arrays.toString(colTypes));

		statement.close();

		sql += ") VALUES (";

		StringTokenizer tokenizer = new StringTokenizer(line, ",");

		while (tokenizer.hasMoreElements()) {
			tokenizer.nextElement();
			sql += "?";

			if (tokenizer.hasMoreElements()) {
				sql += ",";
			}
		}

		sql += ")";

		return sql;
	}

	public void handleLine(String table, String line, String dateformat, String filePath) throws Exception {
		String sql;
		PreparedStatement statement;
		sql = prepareSql(table, line);
		statement = prepareStatement(sql, line, dateformat, filePath);
		
		statement.execute();
		statement.cancel();
		statement.close();
	}

	private PreparedStatement prepareStatement(String sql, String line, String dateformat, String filePath) throws Exception {
		PreparedStatement statement = connection.prepareStatement(sql);

		StringTokenizer tokenizer = new StringTokenizer(line, ",");

		int index = 1;
		while (tokenizer.hasMoreElements()) {
			String element = (String) tokenizer.nextElement();
			element = StringUtils.decodeString(element);
			
			String type = colTypes[index-1];
			
			if (element.equals("NULL"))
				statement.setString(index, null);
			else if( type.equals("java.math.BigDecimal") ) {
				statement.setBigDecimal(index, new BigDecimal(element));
			} 
			else if ( type.equals("java.lang.String") ) {
				statement.setString(index, element);
			}
			else if ( type.equals("oracle.sql.TIMESTAMP")) {
				if(element!=null && !element.equalsIgnoreCase("null")){
					java.util.Date date = StringUtils.getValidDate(element, dateformat);
					Timestamp timestamp = new Timestamp(date.getTime());
					statement.setTimestamp(index, timestamp);
				}
				else{
					statement.setTimestamp(index, null);
				}	
			} 
			else if ( type.equals("oracle.jdbc.OracleClob") ) {
				handleClob(filePath, statement, index, element);
			}
			else if ( type.equals("oracle.jdbc.OracleBlob") ) {
				handleBlob(filePath, statement, index, element);
			}
			else{
				statement.setString(index, element);
			}

//			System.out.println(type + " - " + element);

			index++;
		}
//		System.out.println(sql+"\n");		

		return statement;
	}

	private void handleClob(String filePath, PreparedStatement statement, int index, String element)
			throws FileNotFoundException, SQLException {
		if(element!=null && !element.equalsIgnoreCase("null")){
			String fileName = element.substring(5);
			File file = new File(filePath+"/"+fileName);
//				file.deleteOnExit();
			InputStream fis = new FileInputStream(file);
			statement.setAsciiStream(index, fis);
		}
		else{
			statement.setAsciiStream(index, null, 0);
		}
	}
	
	private void handleBlob(String filePath, PreparedStatement statement, int index, String element)
			throws FileNotFoundException, SQLException {
		if(element!=null && !element.equalsIgnoreCase("null")){
			String fileName = element.substring(5);
			File file = new File(filePath+"/"+fileName);
//				file.deleteOnExit();
			InputStream fis = new FileInputStream(file);
			statement.setBlob(index, fis);
		}
		else{
			statement.setBlob(index, null, 0);
		}
	}

	public void performTruncate(String table ) throws SQLException {
		String sql = "TRUNCATE TABLE " + table;
		Statement statement = connection.createStatement();
		statement.executeUpdate(sql);
		statement.close();
	}
}
