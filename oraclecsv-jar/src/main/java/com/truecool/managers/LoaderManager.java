package com.truecool.managers;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Driver;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.truecool.sql.GenericSQLReader;

/**
 * Loads data from CSV into DB tables
 * 
 * @author Alberto Lagna
 *
 */
public class LoaderManager extends BaseManager{

	private static String START_QRY = "select 'ALTER TABLE '||substr(c.table_name,1,35)|| ' ";
	private static String END_QRY = 
		" CONSTRAINT '||constraint_name||' ;' from user_constraints c, user_tables u where c.table_name = u.table_name;";
	private static String ENABLE_QRY = START_QRY + "ENABLE" + END_QRY;
	private static String DISABLE_QRY = START_QRY + "DISABLE" + END_QRY;
	

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
		startConnection();
		try {
			List<String> fileList = fileList(filePath);
			Date mainStart = new Date();
			
			disableConstraints();
			for (String fileName : fileList) {
				if (fileName.endsWith(".csv")) {
					String tableName = fileName.replace(".csv", "");
					Date start = new Date(); 
					System.out.print("Importing: " + tableName);
					loadData(tableName, filePath + "/" + fileName, true, dateFormat); 
					Date end = new Date();
					System.out.println(" => it took " + (end.getTime() - start.getTime()) + " msecs");
				}
			}
			enableConstraints();
			
			Date mainEnd = new Date();
			System.out.println("------------------------------------------");
			System.out.println("Import completed => it took " + 
				(mainEnd.getTime() - mainStart.getTime()) + " msecs\n");
			
		} catch (Exception e){
			e.printStackTrace();
			System.err.println("Error loading DB: " + e.getMessage());
		}
		closeConnection();
	}
	
	/**
	 * Enable constraints (after the load of all the table content)
	 */
	private void enableConstraints() {
//		manageConstraints(ENABLE_QRY);
	}

	/**
	 * Either enables or disables constraints
	 * @param cmd
	 */
	private void manageConstraints(String cmd) {
		GenericSQLReader reader = new GenericSQLReader(connectionManager.getConnection());
		List data = reader.getData(ENABLE_QRY);
		for (int rowIndex = 0; rowIndex < data.size(); rowIndex++) {
			System.out.println(data.get(rowIndex));
		}
	}

	/**
	 * Disable constraints (to load all the table content)
	 */
	private void disableConstraints() {
//		manageConstraints(DISABLE_QRY);
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
	 * @param table
	 * @param filePath
	 * @param truncate
	 * @param dateformat
	 * @throws Exception
	 */
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
