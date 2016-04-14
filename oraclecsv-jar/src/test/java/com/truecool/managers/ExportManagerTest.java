package com.truecool.managers;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import com.truecool.sql.GenericSQLReader;

import oracle.jdbc.driver.OracleDriver;

public class ExportManagerTest {

	private static final String DATE_FORMAT = "MM/dd/yyyy hh:mm:ss a";
	private static final String SOURCE_DB_URL = "jdbc:oracle:thin:FP_MYENVLOC/D4t4b4s3_myenvloc@192.168.2.10:1521:XE";
	private static final String TARGET_DB_URL = "jdbc:oracle:thin:FP_MYENVLOC_B/D4t4b4s3_myenvloc_b@192.168.2.10:1521:XE";
	private static final String SOURCE_DB_USER = "FP_MYENVLOC";
	private static final String WORK_PATH = "/tmp";
	
	private static final String QUERY_DISABLE_CONSTRAINT = 
		"select 'ALTER TABLE '||substr(c.table_name,1,35)|| ' DISABLE CONSTRAINT '||constraint_name||' ;' from user_constraints c, user_tables u where c.table_name = u.table_name;";

	private ConnectionManager connectionManager = new ConnectionManager();
	
	/**
	 * Gets the list of the tables of the current user
	 * 
	 * @param username
	 * @return
	 * @throws ClassNotFoundException
	 * @throws SQLException
	 */
	private List<String> getTableList(String username) throws ClassNotFoundException, SQLException{
		List<String> res = new ArrayList<String>();
		
		connectionManager.setupConnection(new OracleDriver(), SOURCE_DB_URL);
		GenericSQLReader reader = new GenericSQLReader(connectionManager.getConnection());
		List data = reader.getData("SELECT owner, table_name FROM all_tables");
		if (data != null && data.size() > 0) {
			for (int rowIndex = 0; rowIndex < data.size(); rowIndex++) {
				Map row = (Map) data.get(rowIndex);//				System.out.println(row);
				String owner = row.get("OWNER").toString();
				String tableName = row.get("TABLE_NAME").toString();
				if (owner.equals(SOURCE_DB_USER)){
					res.add(tableName);
				}
			}
		}
		
		return res;
	}
	
	
	@Test
	public void testExportData() {
		try {
			ExportManager exportManager = new ExportManager(new OracleDriver(), SOURCE_DB_URL);
			List<String> tableList = getTableList(SOURCE_DB_USER);
			Date mainStart = new Date();
			for (String tableName : tableList) {
				Date start = new Date(); 
				System.out.print("Exporting: " + tableName);
				exportManager.exportData(tableName, WORK_PATH + "/" + tableName + ".csv", DATE_FORMAT); 
				Date end = new Date();
				System.out.println(" => it took " + (end.getTime() - start.getTime()) + " msecs");
			}
			Date mainEnd = new Date();
			System.out.println("Export completed => it took " + 
				(mainEnd.getTime() - mainStart.getTime()) + " msecs\n");

		} catch (Exception e){
			e.printStackTrace();
			Assert.fail("Error exporting DB: " + e.getMessage());
		}
	}
	
	@Test
	public void testImportData() {
		try {
			// clean DB
			
			// load DB
			LoaderManager loaderManager = new LoaderManager(new OracleDriver(), SOURCE_DB_URL);
			List<String> fileList = fileList(WORK_PATH);
			Date mainStart = new Date();
			for (String fileName : fileList) {
				if (fileName.endsWith(".csv")) {
					String tableName = fileName.replace(".csv", "");
					Date start = new Date(); 
					System.out.print("Importing: " + tableName);
					loaderManager.loadData(tableName, WORK_PATH + "/" + fileName, true, DATE_FORMAT); 
					Date end = new Date();
					System.out.println(" => it took " + (end.getTime() - start.getTime()) + " msecs");
				}
			}
			Date mainEnd = new Date();
			System.out.println("Import completed => it took " + 
				(mainEnd.getTime() - mainStart.getTime()) + " msecs\n");
			
		} catch (Exception e){
			e.printStackTrace();
			Assert.fail("Error loading DB: " + e.getMessage());
		}
	}
	
	private List<String> fileList(String directory) {
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

}
