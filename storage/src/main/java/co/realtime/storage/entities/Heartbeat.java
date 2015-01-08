package co.realtime.storage.entities;

import co.realtime.storage.ext.StorageException;

/**
 * Heartbeat configuration. An heartbeat is a small message periodically sent to the Realtime Messaging servers. When the Realtime Messaging Server detects that a given client isn't sending the heartbeat for a specified period (the default is 3 lost heartbeats) it will consider that the user has disconnected and perform the normal disconnection operations (e.g. decrease the number of subscribers).
 * 
 * @author RTCS Development Team
 *
 */
public class Heartbeat {
	Boolean active;
	Integer fails;
	Integer time;
	
	/**
	 * Retrieves the current state of the heartbeat feature.
	 * 
	 * @return The current active state. 
	 */
	public Boolean getActive() {
		return active;
	}
	
	/**
	 * Enables/Disables the heartbeat feature.
	 * 
	 * @param active The active state.
	 */
	public void setActive(Boolean active) {
		this.active = active;
	}
	
	/**
	 * Retrieves the number of lost messages a server should expect before the servers consider the client has disconnected.
	 * 
	 * @return The number of fails.
	 */
	public Integer getFails() {
		return fails;
	}
	
	/**
	 * Assigns the number of lost messages a server should expect before the servers consider the client has disconnected. Ranges between 1 and 6.
	 * 
	 * @param fails 
	 * 		The number of times, heartbeat messages, are lost before the server considers the client has disconnected.
	 * @throws StorageException 
	 * 		Exception thrown if the value set is outside the allowed range.
	 */
	public void setFails(Integer fails) throws StorageException {
		if(fails > 0 && fails < 7)
			this.fails = fails;
		else
			throw new StorageException("Parameter 'fails' must be between 1 and 6.");
	}
	
	/**
	 * Retrieves the time (seconds) between heartbeat messages sent to the server.
	 * 
	 * @return The time (seconds) between heartbeat messages sent to the server.
	 */
	public Integer getTime() {
		return time;
	}
	
	/**
	 * Assigns the time (seconds) between heartbeat messages sent to the server.
	 * 
	 * @param time 
	 * 		(seconds) Ranges between 10 and 60 seconds.
	 * @throws StorageException 
	 * 		Exception thrown if the value set is outside the allowed range.
	 */
	public void setTime(Integer time) throws StorageException {
		if(time > 9 && time < 61)
			this.time = time;
		else
			throw new StorageException("Parameter 'time' must be between 10 and 60.");
	}
	
	/**
	 * Creates an empty Heartbeat instance.
	 */
	public Heartbeat() {
		active = null;
		fails = null;
		time = null;
	}
	
	/**
	 * Creates a Heartbeat instance with the given configuration.
	 * 
	 * @param active 
	 * 		Enables/Disables the heartbeat feature.
	 * @param fails 
	 * 		The number of times, heartbeat messages, are lost before the server considers the client has disconnected.
	 * @param time 
	 * 		Assigns the time (seconds) between heartbeat messages sent to the server.
	 * @throws StorageException 
	 * 		Exception thrown if any of the specified attributes are outside the allowed range. 
	 */
	public Heartbeat(Boolean active, Integer fails, Integer time) throws StorageException {
		setActive(active);
		setFails(fails);
		setTime(time);
	}
}
