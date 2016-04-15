package com.truecool.managers;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import com.truecool.sql.GenericSQLReader;
import com.truecool.utils.StringUtils;

import oracle.sql.CLOB;
import oracle.sql.TIMESTAMP;

/**
 * Manages the export of the DB
 * 
 * @author Alberto Lagna
 *
 */
public class ExportManager extends BaseManager{


	/**
	 * Constructor with the mandatory fields
	 * @param driver
	 * @param url
	 * @throws Exception
	 */
	public ExportManager(Driver driver, String url) throws Exception {
		super(driver, url);
	}

	/**
	 * Constructor with no fields cannot be used
	 */
	@SuppressWarnings("unused")
	private ExportManager(){
		super();
	}

	
	/**
	 * Exports all the data (all the tables) of the current DB
	 * 
	 * @param targetPath
	 * @param dateFormat
	 * @throws Exception
	 */
	public void exportData(String targetPath, String dateFormat) throws Exception {
		startConnection();
		try {
			cleanDir(targetPath);
			List<String> tableList = getTableList(userName);
			Date mainStart = new Date();
			for (String tableName : tableList) {
				Date start = new Date(); 
				System.out.print("Exporting: " + tableName);
				exportData(tableName, targetPath + "/" + tableName + ".csv", dateFormat, false); 
				Date end = new Date();
				logDebug(" => it took " + (end.getTime() - start.getTime()) + " msecs");
			}
			Date mainEnd = new Date();
			logDebug("------------------------------------------");
			logDebug("Export completed => it took " + 
				(mainEnd.getTime() - mainStart.getTime()) + " msecs\n");

		} catch (Exception e){
			logError("Error exporting DB: " + e.getMessage(), e);
		}
		closeConnection();
	}
	
	
	/**
	 * Used to clean the target dir and if it doesn't exist, to create it
	 * @param targetPath
	 */
	private void cleanDir(String targetPath){
		File targetDir = new File(targetPath);
		
		if (!targetDir.exists())
			targetDir.mkdir();
		else {
			final File[] files = targetDir.listFiles();
			for (int i = 0; i < files.length; i++) {
				files[i].delete();
			}
		}
	}

	/**
	 * Gets the list of the tables of the current user
	 * 
	 * TODO oracle specific
	 * 
	 * @param username
	 * @return
	 * @throws ClassNotFoundException
	 * @throws SQLException
	 */
	private List<String> getTableList(String username) throws ClassNotFoundException, SQLException{
		List<String> res = new ArrayList<String>();
		
		GenericSQLReader reader = new GenericSQLReader(connection);
		List data = reader.getData("SELECT owner, table_name FROM all_tables ORDER BY table_name");
		if (data != null && data.size() > 0) {
			for (int rowIndex = 0; rowIndex < data.size(); rowIndex++) {
				Map row = (Map) data.get(rowIndex);//				logDebug(row);
				String owner = row.get("OWNER").toString();
				String tableName = row.get("TABLE_NAME").toString();
				if (owner.equals(username)){
					res.add(tableName);
				}
			}
		}
		
		return res;
	}
		
	/**
	 * Exports only a single table, used for backward compatibility
	 * 
	 * @param table
	 * @param filePath
	 * @param dateformat
	 * @param singleTableExport
	 * @throws Exception
	 */
	public void exportData(String table, String filePath, String dateformat) throws Exception {
		exportData(table, filePath, dateformat, true);
	}

	
	/**
	 * Exports the data from the table to the filePath
	 * @param table
	 * @param filePath
	 * @param dateformat
	 * @throws Exception
	 */
	public void exportData(String table, String filePath, String dateformat, boolean singleTableExport) throws Exception {
		BufferedWriter writer = null;
		ResultSetMetaData metaData = null;

		try {
			if (singleTableExport)
				startConnection();
			GenericSQLReader reader = new GenericSQLReader(connection);

			// Trying to set an order, to be able to compare exported files
			List data = reader.getData("SELECT * FROM " + table + " ORDER BY 1");

			if (data != null && data.size() > 0) {
				metaData = reader.getTableMetaData(table);
				int columnCount = metaData.getColumnCount();

				writer = new BufferedWriter(new FileWriter(filePath));

				for (int rowIndex = 0; rowIndex < data.size(); rowIndex++) {
					Map row = (Map) data.get(rowIndex);

					for (int colIndex = 0; colIndex < columnCount; colIndex++) {
						String colName = metaData.getColumnName(colIndex + 1);
						String colType = metaData.getColumnClassName(colIndex + 1);
						
						Object value = row.get(colName);

						if ( colType.equals("oracle.jdbc.OracleClob") ) {
							if(value != null){
								value = handleClob((CLOB) value, rowIndex, colIndex);
							}
						} 
						else if (colType.equals("oracle.sql.TIMESTAMP")) {
							if(value != null){
								oracle.sql.TIMESTAMP ts = new TIMESTAMP(value.toString());
								value = handleTimeStamp(ts.timestampValue(),dateformat);
							}
						}

						writer.write((value == null ? "NULL" : StringUtils.encodeString(value.toString()) ));
						writer.write((colIndex < columnCount - 1 ? "," : ""));
					}

					writer.write("\n");
				}
			}
			
		} finally {
			if (writer != null) {
				writer.flush();
				writer.close();
			}
			if (singleTableExport)
				connectionManager.cleanupConnection();
		}
	}

	private String handleClob(CLOB value, int rowIndex, int colIndex) throws Exception {
		String colValue = "clobdata-r" + rowIndex + "c" + colIndex + ".txt";
		File clobDataFile = new File(colValue);

		InputStream in = null;
		OutputStream out = null;

		try {
			byte buffer[] = new byte[(int) value.length()];
			in = value.getAsciiStream();
			in.read(buffer);
			String clobData = new String(buffer);

			out = new FileOutputStream(clobDataFile);
			out.write(clobData.getBytes());
		} finally {
			if (in != null) {
				in.close();
			}

			if (out != null) {
				out.flush();
				out.close();
			}

		}

		return "CLOB=" + colValue;
	}

	private String handleTimeStamp(Timestamp date, String dateformat) {
		String formattedDate = null;
		SimpleDateFormat dateFormat = new SimpleDateFormat(dateformat);
		formattedDate = dateFormat.format(new java.util.Date(date.getTime()));
		return formattedDate;
	}

}
