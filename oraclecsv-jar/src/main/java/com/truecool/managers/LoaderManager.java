package com.truecool.managers;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Driver;
import java.sql.SQLException;
import java.sql.SQLRecoverableException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import com.truecool.sql.GenericSQLReader;

/**
 * Loads data from CSV into DB tables
 * 
 * @author Alberto Lagna
 *
 */
public class LoaderManager extends BaseManager{

	private static final String CONSTRAINT_QRY = "select u.table_name, c.constraint_name from user_constraints c, user_tables u where c.table_name = u.table_name";
	
	/** the max number of accepted constraint loop */
	private static final int MAX_CONSTRAINT_LOOP = 2;
	
	
	/**
	 * Constructor with mandatory fields
	 * 
	 * @param driver
	 * @param url
	 * @throws Exception
	 */
	public LoaderManager(Driver driver, String url) throws Exception {
		super(driver, url);
	}
	
	public LoaderManager(DataSource dataSource) throws Exception{
		super(dataSource);
	}

	public LoaderManager(DataSource dataSource, boolean isDebug) throws Exception{
		super(dataSource, isDebug);
	}

	
	/**
	 * Default constructor is not allowed
	 */
	@SuppressWarnings("unused")
	private LoaderManager(){}

	/**
	 * Loading the data of the whole DB
	 * 
	 * @param table
	 * @param filePath
	 * @param truncate
	 * @param dateFormat
	 * @throws Exception
	 */
	public void loadData(String filePath, boolean truncate, String dateFormat) throws Exception {
		try {
			List<String> fileList = fileList(filePath);
			Date mainStart = new Date();
			
			disableConstraints();
			for (String fileName : fileList) {
				if (fileName.endsWith(".csv")) {
					String tableName = fileName.replace(".csv", "");
					Date start = new Date(); 
					logDebug("Importing: " + tableName);
					startConnection();
					loadData(tableName, filePath, true, dateFormat); 
					closeConnection();
					Date end = new Date();
					logDebug(" => it took " + (end.getTime() - start.getTime()) + " msecs");
				}
			}
			enableConstraints();
			
			Date mainEnd = new Date();
			logDebug("------------------------------------------");
			logDebug("Import completed => it took " + 
				(mainEnd.getTime() - mainStart.getTime()) + " msecs\n");
			
		} catch (Exception e){
			e.printStackTrace();
			logError("Error loading DB: " + e.getMessage(), e);
		}
	}
	
	/**
	 * Enable constraints (after the load of all the table content)
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 */
	private void enableConstraints() throws ClassNotFoundException, SQLException {
		logDebug("Enabling constraints: " + CONSTRAINT_QRY);
		manageConstraints("ENABLE");
	}

	/**
	 * Either enables or disables constraints.
	 * 
	 * In case of enabling constraints, it can happen that we are trying to enable constraints that depends
	 * on other constraints. In this case the further cannot be done before the latter are done: usually a
	 * FK depending on PK. To overcome this problem we keep aside the enable constraint that failed and we retry it
	 * later
	 * 
	 * @param cmd
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 */
	private void manageConstraints(String cmd) throws ClassNotFoundException, SQLException {
		startConnection();

		GenericSQLReader reader = new GenericSQLReader(connection);
		List data = reader.getData(CONSTRAINT_QRY);
		
		int n=1;
		while(n<= MAX_CONSTRAINT_LOOP && !data.isEmpty()){
			logDebug("Constraint loop n " + n++);
			for (Iterator<Object> i=data.iterator(); i.hasNext(); ) {
				Map row = (Map) i.next();
				String tableName = (String)row.get("TABLE_NAME");
				String constraintName = (String)row.get("CONSTRAINT_NAME");
				
				String sql = "ALTER TABLE " + tableName + " " + cmd + " CONSTRAINT " + constraintName;
				Statement statement;
				try {
					statement = connection.createStatement();
					statement.executeUpdate(sql);
	//				logDebug(sql);
					statement.close();
					i.remove();
				} catch (SQLRecoverableException re) {
					logError("SQLRecoverableException: " + re.getMessage());
					return;
				} catch (SQLException e) {
					logError("  Error in executing " + cmd + " of constraint " + sql + ":\n  " + e.getMessage().replace("\n", "") );
					logError("  Retrying when the other constraints are set\n");
				}
			}
		}
		if (!data.isEmpty())
			logError("It was not possible to completely manage all the constraints");
		closeConnection();

		logDebug("Constraint " + cmd + " ended");
	}

	/**
	 * Disable constraints (to load all the table content)
	 * @throws SQLException 
	 * @throws ClassNotFoundException 
	 */
	private void disableConstraints() throws ClassNotFoundException, SQLException {
		logDebug("Disabling constraints: " + CONSTRAINT_QRY);
		manageConstraints("DISABLE");
	}
	

	/**
	 * Lists all the file in a dir
	 * 
	 * @param directory
	 * @return
	 */
	public static List<String> fileList(String directory) {
        List<String> fileNames = new ArrayList<String>();
        try {
        	DirectoryStream<Path> directoryStream = Files.newDirectoryStream(Paths.get(directory));
            for (Path path : directoryStream) {
                fileNames.add(path.getFileName().toString());
            }
        } catch (IOException ex) {
        	ex.printStackTrace();
        }
        return fileNames;
    }
	
	/**
	 * Loading the data of a table
	 * 
	 * @param tableName
	 * @param filePath
	 * @param truncate
	 * @param dateformat
	 * @throws Exception
	 */
	public void loadData(String tableName, String filePath, boolean truncate, String dateformat) throws Exception {
		BufferedReader reader = null;
		FileReader fReader = null;
		try {
			fReader = new FileReader(filePath  + "/" + tableName + ".csv");
			reader = new BufferedReader(fReader);

			if (truncate) {
				connectionManager.performTruncate(tableName);
			}

			String line = null;
			while ((line = reader.readLine()) != null) {
				connectionManager.handleLine(tableName, line, dateformat, filePath);
			}
		} finally {
			if (reader != null) {
				reader.close();
				fReader.close();
			}
		}
	}


}
