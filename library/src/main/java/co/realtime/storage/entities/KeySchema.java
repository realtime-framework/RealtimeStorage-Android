package co.realtime.storage.entities;

import java.util.HashMap;
import java.util.Map;

import co.realtime.storage.StorageRef.StorageDataType;

/**
 * Specification of the key schema meant for a table.
 * @author RTCS Development team
 *
 */
public class KeySchema implements IORMapping {
	private String name;
	private StorageDataType dataType;
	
	/**
	 * Retrieves the name of the key.
	 * @return 
	 * 		the type (Number or String) of the value stored in the key.
	 */
	String getName() {
		return name;
	}
	
	/**
	 * Assigns a name to the key.
	 * @param name The name of the key.
	 */
	void setName(String name) {
		this.name = name;
	}
	
	/**
	 * Retrieves the type (Number or String) of the value stored in the key.
	 * @return 
	 * 		the type (Number or String) of the value stored in the key.
	 */	
	StorageDataType getDataType() {
		return dataType;
	}
	
	/**
	 * Assigns the type (Number or String) of the value stored in the key.
	 * @param dataType 
	 * 		Type of the key.
	 */
	void setDataType(StorageDataType dataType) {
		this.dataType = dataType;
	}
	
	private KeySchema() {}
	
	/**
	 * Builds a key schema.
	 * @param name 
	 * 		Name of the key.
	 * @param dataType 
	 * 		Type of the key.
	 */
	public KeySchema(String name, StorageDataType dataType) {
		this.name = name;
		this.dataType = dataType;
	}
	
	public Map<String, Object> map() {
		Map<String, Object> throughputMap = new HashMap<String, Object>();
		throughputMap.put("name", getName());
		throughputMap.put("dataType", getDataType().name());	
		
		return throughputMap;
	}
}