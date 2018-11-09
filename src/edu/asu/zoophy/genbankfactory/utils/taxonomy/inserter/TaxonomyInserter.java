package edu.asu.zoophy.genbankfactory.utils.taxonomy.inserter;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.apache.log4j.Logger;
import java.util.zip.GZIPInputStream;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.utils.IOUtils;

import jp.ac.toyota_ti.coin.wipefinder.server.database.DBManager;
import jp.ac.toyota_ti.coin.wipefinder.server.utils.ResourceProvider;

/**
 * This insert in a GenBank DB the taxonomy dump from Genbank website.
 * to link the taxonomy entries to the host table by using REs for matching the name to the Taxonomy IDs, run a different program: #HostAligner
 * @author Davy
 */
public class TaxonomyInserter  {
	private static final Logger log = Logger.getLogger("TaxonomyInserter");

	final String INSERT_CONCEPT 	= "INSERT INTO \"Taxonomy_Concept\" VALUES (?,?,?,?)";
	final String INSERT_TREE 		= "INSERT INTO \"Taxonomy_Tree\" VALUES (?,?,?,?,?)";
	final String INSERT_DIVISION 	= "INSERT INTO \"Taxonomy_Division\" VALUES (?,?,?)";
	
	String pathDumpFiles = null;
	BufferedReader brNodes = null;
	BufferedReader brNames = null;
	BufferedReader brDivision = null;

	private Connection conn = null;
	public TaxonomyInserter(String taxDumpPath) throws Exception {			
	    try {
			conn = ((DBManager)ResourceProvider.getResource("DBGenBank")).getConnection();
	    }
	    catch(Exception e) {
	    	log.fatal( "Impossible to Initiate the Resources Provider:"+e.getMessage());
	    	throw new Exception("Impossible to Initiate the Resources Provider:"+e.getMessage());
	    }
		try {
			pathDumpFiles = taxDumpPath;
			//files can be found in the taxdump.tar.gz file at ftp://ftp.ncbi.nlm.nih.gov/pub/taxonomy/ 
			brNodes 	= new BufferedReader(new FileReader(pathDumpFiles+"nodes.dmp"));
			brNames 	= new BufferedReader(new FileReader(pathDumpFiles+"names.dmp"));
			brDivision 	= new BufferedReader(new FileReader(pathDumpFiles+"division.dmp"));
		}
		catch (Exception e) {
			log.fatal( "Error occured when reading the directory containing the files of the dump: "+e.getMessage());
			throw new Exception("Error occured when reading the directory containing the files of the dump: "+e.getMessage());
		}
	}
	
	public void finalize() {
		try{
		if(brNodes!=null)
			brNodes.close();
		if(brNames!=null)
			brNames.close();
		if(brDivision!=null)
			brDivision.close();
		}
		catch(Exception e) {
			//die quietly...
		}
	}
	
	public void insertTaxo() throws Exception {
		insertTaxonomyCpts();
		insertTaxonomyDivisions();
		insertTaxonomyTree();
	}
	
	protected void insertTaxonomyDivisions() throws Exception {
		log.info("=> Start read the division");
		List<List<String>> cpts = new LinkedList<>(); 
		try {
			String line = null;
			while((line = brDivision.readLine())!=null) {
//				log.info(line);
				String[] components = line.split("\t\\|\t");
				if(components.length!=4)
					throw new Exception("There is a problem with the division file format getting ["+components.length+"] column when expected 3: "+line);
				List<String> cpt = new ArrayList<>();
				cpt.add(components[0]);
				cpt.add(components[1]);
				cpt.add(components[2]);
				cpts.add(cpt);
			}
		}
		catch (Exception e) {
			log.fatal( "Error occured when reading the division of the taxonomy in the DB: "+e.getMessage());
			throw new Exception("Error occured when reading the division of the taxonomy in the DB: "+e.getMessage());
		}
		//now we insert the concepts
		PreparedStatement stm = null;
		int count = 0;
		try {
			conn.setAutoCommit(false);
			final int batchSize = 1000;
			log.info("=> start inserting division in batch...");
			stm = conn.prepareStatement(INSERT_DIVISION);
			for(List<String> cpt: cpts) { 
				stm.setInt(1, new Integer(cpt.get(0)));
				stm.setString(2, cpt.get(1));
				stm.setString(3, cpt.get(2));
				stm.addBatch();
				if(count % batchSize == 0)
					stm.executeBatch();
				count++;
			}
			stm.executeBatch();
			conn.setAutoCommit(true);
			log.info("=> Inserted.");
		}
		catch(Exception e) {
			log.fatal( "Impossible to insert the division ["+count+"] in the DB: "+e.getMessage());
			throw new Exception("Impossible to insert the division ["+count+"] in the DB: "+e.getMessage());
		}
		finally {
			try {
				if(stm!=null)
					stm.close();
			} 
			catch (SQLException e) {
				log.fatal( "Error occurs when closing the resources taken on the genbank DB, nothing done: "+e.getMessage());
			}
		}		
	}
	
	protected void insertTaxonomyTree() throws Exception {
		log.info("=> Start read the tree");
		List<List<String>> cpts = new LinkedList<>(); 
		try {
			String line = null;
			while((line = brNodes.readLine())!=null) {
//				log.info(line);
				String[] components = line.split("\t\\|\t");
				if(components.length!=13 && components[12].endsWith("\t|"))
					throw new Exception("There is a problem with the tree file format getting ["+components.length+"] column when expected 4: "+line);
				List<String> cpt = new ArrayList<>();
				cpt.add(components[0]);
				cpt.add(components[1]);
				cpt.add(components[2]);
				cpt.add(components[3]);
				cpt.add(components[4]);
				cpts.add(cpt);
			}
		}
		catch (Exception e){
			log.fatal( "Error occured when reading the tree of the taxonomy in the DB: "+e.getMessage());
			throw new Exception("Error occured when reading the tree of the taxonomy in the DB: "+e.getMessage());
		}
		//now we insert the concepts
		PreparedStatement stm = null;
		int count = 0;
		try {
			conn.setAutoCommit(false);
			final int batchSize = 1000;
			log.info("=> start inserting tree in batch...");
			stm = conn.prepareStatement(INSERT_TREE);
			for(List<String> cpt: cpts) { 
				stm.setInt(1, new Integer(cpt.get(0)));
				stm.setInt(2, new Integer(cpt.get(1)));
				stm.setString(3, cpt.get(2));
				stm.setString(4, cpt.get(3));
				stm.setInt(5, new Integer(cpt.get(4)));
				stm.addBatch();
				if(count % batchSize == 0)
					stm.executeBatch();
				count++;
			}
			stm.executeBatch();
			conn.setAutoCommit(true);
			log.info("=> Inserted.");
		}
		catch(Exception e) {
			log.fatal( "Impossible to insert the node of tree ["+count+"] in the DB: "+e.getMessage());
			throw new Exception("Impossible to insert the node of tree ["+count+"] in the DB: "+e.getMessage());
		}
		finally {
			try {
				if(stm!=null)
					stm.close();
			} 
			catch (SQLException e) {
				log.fatal( "Error occurs when closing the resources taken on the genbank DB, nothing done: "+e.getMessage());
			}
		}		
	}
	
	/**
	 * Read the file and insert the concepts in the DB
	 * @throws Exception
	 */
	protected void insertTaxonomyCpts() throws Exception
	{
		log.info("=> Start read the concepts");
		List<List<String>> cpts = new LinkedList<>(); 
		try {
			String line = null;
			while((line = brNames.readLine())!=null) {
//				log.info(line);
				String[] components = line.split("\t\\|\t");
				if(components.length!=4 && components[3].endsWith("\t|"))
					throw new Exception("There is a problem with the Name file format getting ["+components.length+"] column when expected 4: "+line);
				List<String> cpt = new ArrayList<>();
				cpt.add(components[0]);
				cpt.add(components[1]);
				cpt.add(components[2]);
				cpt.add(components[3].substring(0, components[3].length()-2));
				cpts.add(cpt);
			}
		}
		catch (Exception e) {
			log.fatal( "Error occured when reading the names of the concepts in the DB: "+e.getMessage());
			throw new Exception("Error occured when reading the names of the concepts in the DB: "+e.getMessage());
		}
		//now we insert the concepts
		PreparedStatement stm = null;
		int count = 0;
		try {
			conn.setAutoCommit(false);
			final int batchSize = 1000;
			log.info("=> start inserting concept in batch...");
			stm = conn.prepareStatement(INSERT_CONCEPT);
			for(List<String> cpt: cpts) { 
				stm.setInt(1, new Integer(cpt.get(0)));
				stm.setString(2, cpt.get(1));
				stm.setString(3, cpt.get(2));
				stm.setString(4, cpt.get(3));
				stm.addBatch();
				if(count % batchSize == 0)
					stm.executeBatch();
				count++;
			}
			stm.executeBatch();
			conn.setAutoCommit(true);
			log.info("=> Inserted.");
		}
		catch(Exception e) {
			log.fatal( "Impossible to insert the concept ["+count+"] in the DB: "+e.getMessage());
			throw new Exception("Impossible to insert the concept ["+count+"] in the DB: "+e.getMessage());
		}
		finally {
			try {
				if(stm!=null)
					stm.close();
			} 
			catch (SQLException e) {
				log.fatal( "Error occurs when closing the resources taken on the genbank DB, nothing done: "+e.getMessage());
			}
		}		
	}

	/**
	 * Downloads new taxonomy files from NCBI
	 * @throws Exception 
	 */
	public static void downloadNewTree(String downloadURL, String taxonomyDir) throws Exception {
		try {
			log.info("Downloading new Taxonomy files..");
			String dir = taxonomyDir;
			String fileUrl = downloadURL;
			InputStream in = null;
		    FileOutputStream fout = null;
	    	log.info("Downloading taxdump.tar.gz...");
	    	URL url = new URL(fileUrl);
	        in = new BufferedInputStream(url.openStream());
	        fout = new FileOutputStream(dir+"taxdump.tar.gz");
	        final byte data[] = new byte[1024];
	        int count;
	        while ((count = in.read(data)) != -1) {
	            fout.write(data, 0, count);
	        }
	        fout.flush();
	        in.close();
	        fout.close();
	        log.info("Finished downloading taxdump.tar.gz. Exctracting files from TAR...");
	        TarArchiveInputStream tarIn = new TarArchiveInputStream(new GZIPInputStream(new FileInputStream(dir+"taxdump.tar.gz")));
	        TarArchiveEntry entry = tarIn.getNextTarEntry();
	        while (entry != null) {
	        	if (entry.getName().equals("nodes.dmp") || entry.getName().equals("names.dmp") || entry.getName().equals("division.dmp")) {
	        		log.info("Extracting: "+entry.getName());
		            File curfile = new File(dir, entry.getName());
		            OutputStream out = new FileOutputStream(curfile);
		            IOUtils.copy(tarIn, out);
		            out.close();
		            log.info("Extracted: "+entry.getName());
	        	}
	            entry = tarIn.getNextTarEntry();
	        }
	        tarIn.close();
	        log.info("TAR files extracted");
	        log.info("Deleting taxdump.tar.gz");
	        String textDeletePath = dir+"taxdump.tar.gz";
	        Path deletePath = Paths.get(textDeletePath);
			Files.delete(deletePath);
	        log.info("Deleted taxdump.tar.gz");
		}
		catch (Exception e) {
			log.fatal( "Failed to download new Taxonomy files: "+e.getMessage());
			throw e;
		}
	}
	
}