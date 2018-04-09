/**
 * An instance connected to the Database for storage of the Annotations produced by the {@link Pipeline}
 * 
 * @author dw
 */

package edu.asu.zoophy.gbmetadataupdater.db;

import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.SQLException;


import org.apache.log4j.Logger;

import edu.asu.zoophy.gbmetadataupdater.utils.ResourceProvider;
import edu.asu.zoophy.gbmetadataupdater.utils.ResourceProvider.RP_PROVIDED_RESOURCES;

public class DBInterfacer {
	
	private static final Logger log = Logger.getLogger(DBInterfacer.class);
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
	public DBInterfacer(String DBName, String DBHost, String DBUser, String DBPW) throws Exception
	{
		log.info("DB Interfacer initialisation...");
		
		Name = DBName;
		Host = DBHost;
		User = DBUser;
		PW = DBPW;
		
		try 
		{
			Class.forName("org.postgresql.Driver");
			String dbDriver = (String)ResourceProvider.getPropertiesProvider(RP_PROVIDED_RESOURCES.PROPERTIES_PROVIDER).getValue("annotation.DB.dbDriver");
			log.info("=> Open a connection on a Postgresql DataBase.");
			c = DriverManager.getConnection("jdbc:postgresql://"+Host+"/"+Name, User, PW);
			if (c != null)
				log.info("DB Connected.");
			else
			{
				log.error("The connection handle is null...");
				throw new SQLException ("The connection handle is null...");
			}
		}catch(SQLException se) 
		{
			log.fatal("Couldn't connect the DB with options ["+Host+"/"+Name+" "+User+" "+PW+"]:"+se.getMessage());
			throw se;
		}catch(Exception e)
		{
			log.fatal("Other error occured when instantiate the DB:"+e.getMessage());
			throw e;
		}
	}
	
	/**
	 * Return the connection to the DB
	 * @return The connection to an open DB
	 */
	public Connection getConnection(){
		return c;
	}
	
	/**
	 * Close the connection with the Database
	 * @exception just an alert, we continue anyway
	 */
	public void Close()  
	{
		try
		{
			if(c!=null)
			{
				log.info("Try to close the connection with the DB");
				String dbDriver = (String)ResourceProvider.getPropertiesProvider(RP_PROVIDED_RESOURCES.PROPERTIES_PROVIDER).getValue("annotation.DB.dbDriver");
				if(dbDriver.equals("HSQLDB")){				
					c.createStatement().execute("SHUTDOWN");
				}
				c.close();
				c = null;
				log.info("Connection closed");
			}
		}catch(SQLException se)
		{
			log.error("Impossible to close the connection with the db: "+se.getMessage());
			return;
		}catch(Exception e)
		{
			log.error("Other error when closing the DB:"+e.getMessage());
			return;
		}
	}

}





