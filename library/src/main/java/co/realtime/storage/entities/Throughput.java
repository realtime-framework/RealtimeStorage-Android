package co.realtime.storage.entities;

import java.util.HashMap;
import java.util.Map;

/**
 * Specification of a table read and write operation capacity.
 * @author RTCS Development team
 *
 */
public class Throughput implements IORMapping {
	private Integer read;
	private Integer write;
	
	/**
	 * Retrieves the read operations per second of a table.
	 * @return the assigned read units.
	 */
	Integer getRead() {
		return read;
	}
	
	/**
	 * Assigns the number of read units per second.
	 * @param read
	 * 		The number of read units per second.
	 */
	void setRead(int read) {
		this.read = read;
	}
	
	/**
	 * Retrieves the write operations per second of a table.
	 * @return the assigned write units.
	 */	
	Integer getWrite() {
		return write;
	}
	
	/**
	 * Assigns the number of write units per second.
	 * @param read
	 * 		The number of write units per second.
	 */
	void setWrite(int write) {
		this.write = write;
	}
	
	/**
	 * Builds the table throughput.
	 * @param read
	 * 		The number of read units per second.
	 * @param write
	 * 		The number of write units per second.
	 */
	public Throughput(Integer read, Integer write) {
		this.read = read;
		this.write = write;
	}
	
	public Map<String, Object> map() {
		Map<String, Object> throughputMap = new HashMap<String, Object>();
		throughputMap.put("read", getRead());
		throughputMap.put("write", getWrite());	
		
		return throughputMap;
	}
}