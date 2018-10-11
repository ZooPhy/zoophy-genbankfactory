package edu.asu.zoophy.gbmetadataupdater;

import org.apache.log4j.Logger;

import edu.asu.zoophy.gbmetadataupdater.controller.ControllerInt;
import edu.asu.zoophy.gbmetadataupdater.controller.GBLargeMetadataController;
import edu.asu.zoophy.gbmetadataupdater.db.DBInterfacer;
import edu.asu.zoophy.gbmetadataupdater.utils.PropertiesProvider;
import edu.asu.zoophy.gbmetadataupdater.utils.ResourceProvider;
import edu.asu.zoophy.gbmetadataupdater.utils.ResourceProvider.RP_PROVIDED_RESOURCES;

public class GBMetadataUpdater {
	final private static Logger log = Logger.getLogger(GBMetadataUpdater.class);
	ResourceProvider Provider;
	PropertiesProvider pp;
	DBInterfacer dbi;
	ControllerInt controller;
	
	public GBMetadataUpdater() throws Exception {
		try {
			pp = new PropertiesProvider("GBMetadataUpdater.local.properties");
		    try
		    {
		    	Provider = new ResourceProvider();
		    	ResourceProvider.addResource(RP_PROVIDED_RESOURCES.PROPERTIES_PROVIDER, pp);
		    	dbi = new DBInterfacer((String)pp.getValue("annotation.DB.Name"), (String)pp.getValue("annotation.DB.Host"), (String)pp.getValue("annotation.DB.User"), (String)pp.getValue("annotation.DB.PW"));
		    	ResourceProvider.addResource(RP_PROVIDED_RESOURCES.ANNOTATION_DBI, dbi);
		    }catch(Exception e)
		    {
		    	log.fatal("Impossible to Initiate the Resources Provider:"+e.getMessage());
		    	throw new Exception(e.getMessage());
		    }
		} catch (Exception e) {
			log.error("Impossible to instantiate Geoname Extractor: "+e.getMessage());
			throw new Exception(e.getMessage());
		}
	}
	
	public void run() throws Exception{
		try  {
			controller = new GBLargeMetadataController();
			
			controller.run();
			dbi.Close();
		} catch (Exception e) {
			log.fatal(e.getMessage());
			dbi.Close();
			throw new Exception("Exiting Run method due to Exception " +e.getMessage());
		}
	}
	public static void main (String[] args) throws Exception {
		try {
			GBMetadataUpdater gbu = new GBMetadataUpdater();
			gbu.run();	
		} catch (Exception e) {
			log.fatal(e.getMessage());
		}
		
	}
}
