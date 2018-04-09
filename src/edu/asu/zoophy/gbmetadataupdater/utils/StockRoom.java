package edu.asu.zoophy.gbmetadataupdater.utils;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

/**
 * This utility help to store some objects momently in the memory
 * The main use is to pass through the annotators object which can be reused by the further annotators (ex. Gate document)
 * @author dw
 *
 */
public class StockRoom 
{
	private static final Logger log = Logger.getLogger(StockRoom.class);
	
	/**
	 * The storage is a Hash Map String->Object
	 */
	protected Map<String, Object> storage = null;
	/**
	 * Create a StockRoom with an empty storage
	 */
	public StockRoom()
	{
		storage = new HashMap<String, Object>();
	}
	
	/**
	 * Store a new object in the StoreRoom (delete any other existing Object with the same name)
	 * @param objectName
	 * @param objectReference
	 */
	public void Store(String objectName, Object objectReference)
	{
		storage.put(objectName, objectReference);
	}
	/**
	 * Retrieve an object stored in the room, null if the object doesn't exist
	 * @param objectName
	 * @return the ObjectReference
	 */
	public Object Get(String objectName)
	{
		if(storage.containsKey(objectName))
			return storage.get(objectName);
		log.debug("The object ["+objectName+"] doesn't exist in the room, return Null");
		return null;
	}
	/**
	 * Clean the room by deleting all objects stored
	 */
	public void Clean()
	{
		storage.clear();
	}
}
