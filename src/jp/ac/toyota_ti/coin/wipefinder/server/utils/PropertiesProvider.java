/**
 * This class provides all information requested for the system in a local file 
 * @author dw
 * @version 0.1
 */

package jp.ac.toyota_ti.coin.wipefinder.server.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Enumeration;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This object open the properties file and distribute the properties on request
 * @author dw
 *
 */
public class PropertiesProvider {
	
    private static final Logger log = Logger.getLogger("PropertiesProvider");
	private Properties table;
	
	/**
	 * The local file containing the properties, default value local.properties
	 */
	private String properties_file = "local.properties";


	/**
	 * Read a file which contains the properties. Other values maybe include in the constructor.
	 * @throws Exception
	 */
	public PropertiesProvider() throws Exception
	{
		log.info("PropertiesProvider initializing [File:"+properties_file+"]...");
		this.table = new Properties();
		// Add any additional values required		
		this.loadProperties();
		log.info("PropertiesProvider initialized [File:"+properties_file+"]...");
	}
	/**
	 * Read a file which contains the properties. Other values maybe include in the constructor.
	 * @param namePropertiesFile String Name of the properties File
	 * @throws Exception
	 */
	public PropertiesProvider(String namePropertiesFile) throws Exception
	{
		log.info("PropertiesProvider initializing [File:"+namePropertiesFile+"]...");
		properties_file = namePropertiesFile;
		if(this.table==null)
			this.table = new Properties();
		// Add any additional values required		
		this.loadProperties();
		log.info("PropertiesProvider initialized [File:"+namePropertiesFile+"].");
	}

	/**
	 * Load the properties file in memory.
	 * The File is closed when loading is done.
	 * @throws Exception
	 */
	private void loadProperties() throws Exception
	{
		InputStream is=null;
		try
		{
			//we check if the properties_file as a valid path first
			File checkedPath = new File(this.properties_file);
			if(checkedPath.exists() && checkedPath.canRead())
				is = new FileInputStream(new File(this.properties_file));
			else //we search the file according to the location of the .class				
				is = this.getClass().getClassLoader().getResourceAsStream(this.properties_file);
			 
			this.table.load(is);
			log.info("Properties File ["+properties_file+"] was loaded successfully");			
		}catch(Exception e){
			log.log(Level.SEVERE, "Properties File ["+properties_file+"] was not loaded properly: " + e.getMessage());
			throw e;
		}finally
		{
			try
			{
				is.close();
			}catch(IOException e)
			{
				log.log(Level.SEVERE, "Impossible to close the properties file ["+properties_file+"]:"+e.getMessage());
				return;
			}	
		}
	}
	
	public Object getValue(String property){
		if(this.table.containsKey(property)){
			return this.table.getProperty(property);
		}else{
			log.warning("The property value ["+property+"] asked in the properties file ["+properties_file+"] is missing.");
			return null;
		}
	}
	
	public void setValue(String property, Object value)
	{
		this.table.put(property, value);
	}
	
	/**
	 * Merge this Properties Provider with the one given in Parameter, 
	 * if a property already exits the old value is kept
	 */
	public void mergeWith(PropertiesProvider pp)
	{
		Properties newsProperties = pp.getTable();
		Enumeration<?> keys = newsProperties.propertyNames();
		while(keys.hasMoreElements())
		{
			Object elementKey = keys.nextElement();
			//check if the elementKey exist in the current Properties Provider
			if(this.table.containsKey(elementKey))
			{
				log.warning("The property ["+elementKey.toString()+"->"+pp.getValue((String)elementKey)+"] already exist in the Property Provider, the old value ["+getValue((String)elementKey)+"] is kept.");
			}
			else
			{
				this.table.put(elementKey, pp.getValue((String)elementKey));
			}
		}
	}
	protected Properties getTable()
	{
		return this.table;
	}
	public void Close(){
		table.clear();
		table = null;
	}
}
