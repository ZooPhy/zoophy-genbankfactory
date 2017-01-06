package jp.ac.toyota_ti.coin.wipefinder.server.database;

import java.sql.Connection; 
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

import jp.ac.toyota_ti.coin.wipefinder.server.utils.ResourceProvider;
import jp.ac.toyota_ti.coin.wipefinder.server.utils.ResourceProvider.RP_PROVIDED_RESOURCES;

/**
 * Manager with the connection to the DB
 * @author Davy
 */
public class DBManager {
	private final Logger log = Logger.getLogger("DBManager");
	private String Name = null;
	private String Host = null;
	private String User = null;
	private String PW = null;
	private Connection c = null; 
	/**
	 * Create a Connection to the Database, where the details are provided by the properties provider open in the Dataprovider
	 * @param namePropertieProvider the name of the properties provider in the DataProvider
	 * @throws Exception
	 */
	public DBManager() throws Exception {
		log.info("DB Interfacer initialisation...");
		Name = (String)ResourceProvider.getPropertiesProvider(RP_PROVIDED_RESOURCES.PROPERTIES_PROVIDER).getValue("DB.Big.Name");
		Host = (String)ResourceProvider.getPropertiesProvider(RP_PROVIDED_RESOURCES.PROPERTIES_PROVIDER).getValue("DB.Host");
		User = (String)ResourceProvider.getPropertiesProvider(RP_PROVIDED_RESOURCES.PROPERTIES_PROVIDER).getValue("DB.User");
		PW =   (String)ResourceProvider.getPropertiesProvider(RP_PROVIDED_RESOURCES.PROPERTIES_PROVIDER).getValue("DB.PW");
		try {
			c = DriverManager.getConnection("jdbc:postgresql://"+Host+"/"+Name, User, PW);
			if (c != null) {
				log.info("GenBank DB Connected: "+Name);
			}
			else {
				log.log(Level.SEVERE, "Error occurred when connecting to the GenBank DB: null handler...");
				throw new SQLException ("Error occurred when connecting to the GenBank DB: null handler...");
			}
		}
		catch(SQLException se) {
			log.log(Level.SEVERE, "Couldn't connect the postgres GenBank DB with options [jdbc:postgresql://"+Host+"/"+Name+" "+User+" "+PW+"]:"+se.getMessage());
			throw se;
		}
		catch(Exception e) {
			log.log(Level.SEVERE, "Other error occured when instantiate the GenBank DB:"+e.getMessage());
			throw e;
		}
	}
	/**
	 * Return the connection to the DB
	 * @return The connection to an open DB
	 */
	public Connection getConnection() {
		return c;
	}
	/**
	 * Close the connection with the Database
	 * @exception just an alert, we continue anyway
	 */
	public void Close() {
		try {
			if(c!=null) {
				log.info("Try to close the connection with the GenBank DB: "+Name);
				c.close();
				c = null;
				log.info("Connection closed");
			}
		}
		catch(SQLException se) {
			log.log(Level.SEVERE, "Impossible to close the connection with the GenBank DB: "+se.getMessage());
			return;
		}
		catch(Exception e) {
			log.log(Level.SEVERE, "Other error when closing the GenBank DB:"+e.getMessage());
			return;
		}
	}
	
	public void connectToNewDB(String DBname) throws Exception {
		if (c!=null) {
			Close();
			Name = DBname;
			try {
				//129.219.151.27
				c = DriverManager.getConnection("jdbc:postgresql://"+Host+"/"+Name, User, PW);
				if (c != null) {
					log.info("New connection to DB: "+Name);
				}
				else {
					log.log(Level.SEVERE, "Error occurred when connecting to the GenBank DB: null handler...");
					throw new SQLException ("Error occurred when connecting to the GenBank DB: null handler...");
				}
			}
			catch(SQLException se) {
				log.log(Level.SEVERE, "Couldn't connect the postgres GenBank DB with options [jdbc:postgresql://"+Host+"/"+Name+" "+User+" "+PW+"]:"+se.getMessage());
				throw se;
			}
			catch(Exception e) {
				log.log(Level.SEVERE, "Other error occured when instantiate the GenBank DB:"+e.getMessage());
				throw e;
			}
		}
		else {
			throw new Exception("ERROR! Was not connected to DB to begin with...");
		}
	}
}