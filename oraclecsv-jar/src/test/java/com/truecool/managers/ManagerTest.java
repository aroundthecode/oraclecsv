package com.truecool.managers;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;

import org.junit.Assert;
import org.junit.Test;

import oracle.jdbc.driver.OracleDriver;

public class ManagerTest {

	private static final String DATE_FORMAT = "MM/dd/yyyy hh:mm:ss a";
	private static final String SOURCE_DB_URL = "jdbc:oracle:thin:FP_MYENVLOC/D4t4b4s3_myenvloc@192.168.2.10:1521:XE";
	private static final String TARGET_DB_URL = SOURCE_DB_URL;
	private static final String WORK_PATH = "/tmp/DBexport";
	
	@Test
	public void testExportAndLoadData() {
		// first export
		try {
			ExportManager exportManager = new ExportManager(new OracleDriver(), SOURCE_DB_URL);
			exportManager.exportData(WORK_PATH, DATE_FORMAT);
		} catch (Exception e){
			e.printStackTrace();
			Assert.fail("Error exporting DB: " + e.getMessage());
		}
		try {
			LoaderManager loaderManager = new LoaderManager(new OracleDriver(), SOURCE_DB_URL);
			loaderManager.loadData(WORK_PATH, true, DATE_FORMAT); 
		} catch (Exception e){
			e.printStackTrace();
			Assert.fail("Error loading DB: " + e.getMessage());
		}
		
		// second export
		try {
			ExportManager exportManager = new ExportManager(new OracleDriver(), SOURCE_DB_URL);
			exportManager.exportData(WORK_PATH+"bis", DATE_FORMAT);
		} catch (Exception e){
			e.printStackTrace();
			Assert.fail("Error exporting DB: " + e.getMessage());
		}
		
		// check that the files of the second export are equals to the files of the first
		// since the relation tables cannot be ordered by the ID of the table, if two csv differ in content
		// the size is kept as only feature to check
		System.out.println("Comparing the two exports");
		for (String fileName : LoaderManager.fileList(WORK_PATH+"bis")) {
			String path1 = WORK_PATH+"/"+fileName;
			String path2 = WORK_PATH+"bis" +"/"+fileName;
			System.out.println(path1 + " <-> " + path2);
			
			try {
				if (!sameContent(Paths.get(new URI("file:///"+path1)), Paths.get(new URI("file:///"+path2))))
					System.out.println("   WARNING the two files do not have the same content");
				Assert.assertTrue("Files " + path1 + " and " + path2 + 
					" do not have even the same size ", 
					sameSize(Paths.get(new URI("file:///"+path1)), Paths.get(new URI("file:///"+path2))));
			} catch (IOException | URISyntaxException e) {
				e.printStackTrace();
				Assert.fail("Error comparing files: " + e.getMessage());
			}
		}
	}
	
	/**
	 * Compares the content of two files
	 * @param file1
	 * @param file2
	 * @return
	 * @throws IOException
	 */
	private boolean sameContent(Path file1, Path file2) throws IOException {
	    if (!sameSize(file1, file2))
	    	return false;
		final long size = Files.size(file1);
	    if (size < 4096)
	        return Arrays.equals(Files.readAllBytes(file1), Files.readAllBytes(file2));

	    try (InputStream is1 = Files.newInputStream(file1);
	         InputStream is2 = Files.newInputStream(file2)) {
	        // Compare byte-by-byte.
	        // Note that this can be sped up drastically by reading large chunks
	        // (e.g. 16 KBs) but care must be taken as InputStream.read(byte[])
	        // does not neccessarily read a whole array!
	        int data;
	        while ((data = is1.read()) != -1)
	            if (data != is2.read())
	                return false;
	    }

	    return true;
	}

	private boolean sameSize(Path file1, Path file2) throws IOException {
		final long size = Files.size(file1);
	    if (size != Files.size(file2))
	        return false;
		return true;
	}
}