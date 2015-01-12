package co.realtime.storage.entities;

import java.util.HashMap;
import java.util.Map;

/**
 * Specification of the key structure of a table.
 * @author RTCS Development team
 *
 */
public class Key implements IORMapping {
	private KeySchema primaryKey;
	private KeySchema secondaryKey;
	
	/**
	 * Builds a key structure with a primary key.
	 * @param primaryKey 
	 * 		The primary key schema.
	 */
	public Key(KeySchema primaryKey) {
		this.primaryKey = primaryKey;
	}
	
	/**
	 * Builds a key structure with primary and secondary keys.
	 * @param 
	 * 		primaryKey The primary key schema.
	 * @param 
	 * 		secondaryKey The secondary key schema.
	 */	
	public Key(KeySchema primaryKey, KeySchema secondaryKey) {
		this.primaryKey = primaryKey;
		this.secondaryKey = secondaryKey;
	}

	/**
	 * Retrieves the primary key schema.
	 * @return 
	 * 		primary key schema.
	 */
	KeySchema getPrimaryKey() {
		return primaryKey;
	}

	/**
	 * Assigns the primary key schema.
	 * @param primaryKey 
	 * 		The primary key schema.
	 */
	void setPrimaryKey(KeySchema primaryKey) {
		this.primaryKey = primaryKey;
	}
	
	/**
	 * Retrieves the secondary key schema.
	 * @return 
	 * 		secondary key schema.
	 */
	KeySchema getSecondaryKey() {
		return secondaryKey;
	}

	/**
	 * Assigns the secondary key schema.
	 * @param secondary 
	 * 		The secondary key schema.
	 */
	void setSecondaryKey(KeySchema secondaryKey) {
		this.secondaryKey = secondaryKey;
	}
	
	public Map<String, Object> map() {
		Map<String, Object> throughputMap = new HashMap<String, Object>();
		throughputMap.put("primary", getPrimaryKey().map());
		
		if(getSecondaryKey() != null)
			throughputMap.put("secondary", getSecondaryKey().map());	
		
		return throughputMap;
	}
}