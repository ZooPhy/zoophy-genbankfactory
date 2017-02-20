package jp.ac.toyota_ti.coin.wipefinder.server.utils;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A convenient class to store and retrieve all resources needed like DB, Properties Files, SockRoom...
 * Usage: By default the resource provider can gives access to the Database Interface, the default properties File and the Taxonomy provider.
 * Each resource has to be added to the provider, ex:
 * DBInterfacer dbi = new DBInterfacer((String)pp.getValue("annotation.DB.Name"), (String)pp.getValue("annotation.DB.Host"), (String)pp.getValue("annotation.DB.User"), (String)pp.getValue("annotation.DB.PW"));
 * Provider.addResource(RP_PROVIDED_RESOURCES.ANNOTATION_DBI, dbi);
 * Or
 * WikipediaCategorizer Categorizer = new JWPLWikipediaCategorizer();
 * Provider.addResource("WipeFinderCategorizer", Categorizer);
 * Then the resources can be accessed as follow:
 * (String)ResourceProvider.getPropertiesProvider(RP_PROVIDED_RESOURCES.PROPERTIES_PROVIDER).getValue("mysqlDBName");
 * Or
 * (WikipediaCategorizer)ResourceProvider.getPropertiesProvider("WipeFinderCategorizer");
 * @author dw
 *
 */
public class ResourceProvider 
{	
	private static final Logger log = Logger.getLogger("ResourceProvider");

	/**
	 * The Enum types to the resources known and provided by default by the Resource Provider
	 * @author dw
	 *
	 */
	public static enum RP_PROVIDED_RESOURCES{
		ANNOTATION_DBI,//the default Interface to the DB which contain the corpus annotated to work on
		PROPERTIES_PROVIDER,//the default properties files associated with the current program
		TAXONOMY_PROVIDER,//the default taxonomy interface
		LEXICON;//the default lexicon interface
	}
	/**
	 * The main Map which is used to store all Resources
	 */
	protected static Map<String, Object> resourcesMap = null;
	/**
	 * A common stock room
	 * (instantiate when it's retrieved for the first time to the ResourceProviser)
	 */

	/**
	 * Create a ResourceProvider with a default {@link #resourcesMap}, 
	 * if another {@link ResourceProvider} exists, the previous resourceMap is used. 
	 */
	public ResourceProvider()
	{
		if(resourcesMap==null)
			resourcesMap = new HashMap<String, Object>();
		else
			log.warning("Another ResourceProvider has been created.");
	}
	
	/**
	 * Search a {@link PropertiesProvider} with the given Name and return it 
	 * @param resourceName
	 * @return the PropertiesProvider or null if it doesn't exist
	 */
	public static PropertiesProvider getPropertiesProvider(String resourceName)
	{
		try
		{
			if(!resourcesMap.containsKey(resourceName))
				log.warning("The resource ["+resourceName+"] doesn't exist in the ResourceProvider, null is returned...");
			return (PropertiesProvider)resourcesMap.get(resourceName);
		}catch(Exception e)
		{
			log.log(Level.SEVERE, "Error occured when searching for a PropertiesProvider in the ResourceProvider, null is returned: "+e.getMessage());
			return null;
		}
	}
	/**
	 * Search a {@link PropertiesProvider} to the default properties file i.e. PROPERTIES_PROVIDER  
	 * @param pPropertiesProvider
	 * @return the default properties, null if it doesn't exist or if something else is asked
	 */
	public static PropertiesProvider getPropertiesProvider(Enum<RP_PROVIDED_RESOURCES> pPropertiesProvider)
	{
		try
		{
			if(!resourcesMap.containsKey(pPropertiesProvider.name())||pPropertiesProvider!=RP_PROVIDED_RESOURCES.PROPERTIES_PROVIDER)
				log.warning("The default Properties Provider ["+pPropertiesProvider+"] doesn't exist in the ResourceProvider, null is returned...");
			return (PropertiesProvider)resourcesMap.get(pPropertiesProvider.name());
		}catch(Exception e)
		{
			log.log(Level.SEVERE, "Error occured when searching for the default PropertiesProvider in the ResourceProvider, null is returned: "+e.getMessage());
			return null;
		}
	}
	
	/**
	 * Retrieve the resource with the given name
	 * @param resourceName
	 * @return the resource, null if not find
	 */
	public static Object getResource(String resourceName)
	{
		try
		{
			return resourcesMap.get(resourceName);
		}catch (Exception e) {
			log.log(Level.SEVERE, "Error occured when searching for the resource ["+resourceName+"] in the ResourceProvider, null is returned: "+e.getMessage());
			return null;
		}
	}
	/**
	 * Add a new resource to the {@link ResourceProvider}
	 * If an existing resource exist nothing is done
	 * @param resourceName
	 * @return the resource added
	 */
	public static Object addResource(String resourceName, Object resource)
	{
		log.info("- Adding new resource in the ResourceProvider: "+resourceName);
		if(resourcesMap.containsKey(resourceName))
		{
			log.warning("An existing resource is registered with this name ["+resourcesMap.get(resourceName).getClass().getName()+"], nothing done.");
			return resourcesMap.get(resourceName);
		}
		resourcesMap.put(resourceName, resource);
		return resource;
	}
	/**
	 * Add a new provided resources to the {@link ResourceProvider}
	 * If an new DBI is added while a DBI is already registered nothing is done (use {@link #removeResource(Enum<RP_PROVIDED_RESOURCES> pProvidedResource)} before)
	 * If an new PropertiesProvider is added while a PropertiesProvider is already registered we merge the values (if an existing properties have the same name, the first one is kept)
	 * @param pProvidedResource type of resource being provided
	 * @param resource - resource to add
	 * @return the resource added
	 */
	static public Object addResource(Enum<RP_PROVIDED_RESOURCES> pProvidedResource, Object resource)
	{
		log.info("- Adding a Default Resource to the ResourceProvider: "+pProvidedResource.name());
		if(!resourcesMap.containsKey(pProvidedResource.name()))
		{
			resourcesMap.put(pProvidedResource.name(), resource);
			return resource;
		}
		if(pProvidedResource==RP_PROVIDED_RESOURCES.PROPERTIES_PROVIDER)
		{
			log.warning("An existing Properties Provider is already registered we merge them.");
			getPropertiesProvider(RP_PROVIDED_RESOURCES.PROPERTIES_PROVIDER).mergeWith((PropertiesProvider)resource);
			return getPropertiesProvider(RP_PROVIDED_RESOURCES.PROPERTIES_PROVIDER);
		}
		else
		{
			log.warning("An existing resource is registered with this name ["+pProvidedResource.name()+"], nothing done.");
			return resourcesMap.get(pProvidedResource.name());
		}

	}
	/**
	 * Remove a resource from the provider, nothing done if the resource doesn't exist
	 * @param pProvidedResource - resource to remove
	 * @return the resource removed, null if it doesn't exist
	 */
	static public Object removeResource(Enum<RP_PROVIDED_RESOURCES> pProvidedResource)
	{
		log.info("- REMOVING a Default Resource to the ResourceProvider: "+pProvidedResource.name());
		if(!resourcesMap.containsKey(pProvidedResource.name()))
		{
			log.info("The resource doesn't exist, nothing done.");
			return null;
		}
		else
		{
			return resourcesMap.remove(pProvidedResource.name());
		}
	}
	/**
	 * Remove a resource from the Provider
	 * @param resourceName
	 * @return the resource removed, null if the resource doesn't exists
	 */
	public static Object removeResource(String resourceName)
	{
		if(resourcesMap.containsKey(resourceName))
			return resourcesMap.remove(resourceName);
		log.warning("The resource ["+resourceName+"] doesn't exist in the ReosurceProvider,  null is returned.");
		return null;
	}
	
	/**
	 * Close all resources open by the DataProvider
	 */
	public void Close()
	{
		if(resourcesMap!=null){
			for(Object resources: resourcesMap.values())
			{
				if(resources instanceof PropertiesProvider){
					//do nothing it's close later
				}
				else{
					log.info("Warning, I found a resource ["+resources.getClass().getSimpleName()+"] which is not close properly.");
				}
			}
			//I close the PropertiesProvider at the end since I may need some information inside to close the other resources
			PropertiesProvider pp = (PropertiesProvider)resourcesMap.get(RP_PROVIDED_RESOURCES.PROPERTIES_PROVIDER);
			if(pp!=null){
				pp.Close();
			}
			resourcesMap.clear();
			resourcesMap = null;
		}
	}
	
}



