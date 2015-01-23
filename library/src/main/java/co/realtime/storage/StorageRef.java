package co.realtime.storage;


import android.content.Context;
import co.realtime.storage.Rest.RestType;
import co.realtime.storage.entities.Heartbeat;
import co.realtime.storage.ext.OnBooleanResponse;
import co.realtime.storage.ext.OnConnected;
import co.realtime.storage.ext.OnError;
import co.realtime.storage.ext.OnHeartbeat;
import co.realtime.storage.ext.OnReconnected;
import co.realtime.storage.ext.OnReconnecting;
import co.realtime.storage.ext.OnTableSnapshot;
import co.realtime.storage.ext.StorageException;

/**
 * Class with the definition of a storage reference.
 */
public class StorageRef {
	StorageContext context;

	enum StorageOrder { NULL, ASC, DESC };
	
	/**
	 * Storage provision load
	 */
	public enum StorageProvisionLoad {
		/**
		 * Assign more read capacity than write capacity.
		 */
		READ(1), 
		/**
		 * Assign more write capacity than read capacity.
		 */
		WRITE(2),
		/**
		 * Assign similar read an write capacity.
		 */
		BALANCED(3),
		/**
		 * Assign custom read an write capacity.
		 */
		CUSTOM(4);
		int value;
		private StorageProvisionLoad(int value){
			this.value = value;
		}
		public int getValue(){
			return this.value;
		}
	}
	
	/**
	 * Storage provision type
	 */
	public enum StorageProvisionType {
		/**
		 * 26 operations per second
		 */
		LIGHT(1), 
		/**
		 * 50 operations per second 
		 */
		MEDIUM(2),
		/**
		 * 100 operations per second
		 */
		INTERMEDIATE(3),
		/**
		 * 200 operations per second
		 */
		HEAVY(4),
		/**
		 * customized read and write throughput 
		 */
		CUSTOM(5);
		int value;
		private StorageProvisionType(int value){
			this.value = value;
		}
		public int getValue(){
			return this.value;
		}
	}
	/**
	 * Storage data type, used for definitions of primary and secondary keys
	 */
	public enum StorageDataType {
		/**
		 * Instance of String
		 */
		STRING("string"),
		/**
		 * Instance of Number
		 */
		NUMBER("number");
		private final String dataType;		
		private StorageDataType(String s){
			dataType = s;
		}		
		public String toString() {
			return dataType;
		}
		public static StorageDataType fromString(String s){
			if(s.equals("string")) return StorageDataType.STRING;
			if(s.equals("number")) return StorageDataType.NUMBER;
			return null;		
		}
	};
	
	/**
	 * Storage event types, used for define notifications types
	 */
	public enum StorageEvent {
		/**
		 * On new storage item
		 */
		PUT,
		/**
		 * When storage item is being updated
		 */
		UPDATE,
		/**
		 * When storage item is being deleted
		 */
		DELETE;
		public static StorageEvent fromString(String s){
			if(s.equals("put")) return StorageEvent.PUT;
			if(s.equals("delete")) return StorageEvent.DELETE;
			if(s.equals("update")) return StorageEvent.UPDATE;
			return null;
		}
	};
	
	/**
	 * Creates a new Storage reference.
	 * 
	 * @param applicationKey 
	 * 		The application key
	 * @param privateKey
	 * 		The private key
	 * @param authenticationToken
	 * 		The authentication token.
	 * @deprecated All private key references will be removed in the upcoming library versions. Private key is meant only for server-side use.
	 * Use {@link StorageRef(String applicationKey, String authenticationToken)} instead.
	 */
	@Deprecated
	public StorageRef(String applicationKey, String privateKey, String authenticationToken) throws StorageException{
		this(applicationKey, privateKey, authenticationToken, true, false, "http://storage-balancer.realtime.co/server/1.0");
	}
	
	/**
   * Creates a new Storage reference.
   * 
   * @param applicationKey 
   *    The application key
   * @param privateKey
   *    The private key
   * @param authenticationToken
   *    The authentication token.
   * @param googleProjectId
   *    Your google project id.
   * @param androidApplicationContext
   *    Your android application context.
	 * @deprecated All private key references will be removed in the upcoming library versions. Private key is meant only for server-side use.
	 * Use {@link StorageRef(String applicationKey, String authenticationToken, String googleProjectId, android.content.Context androidApplicationContext)} instead.
   */
	@Deprecated
	public StorageRef(String applicationKey, String privateKey, String authenticationToken, String googleProjectId, Context androidApplicationContext) throws StorageException{
		this(applicationKey, privateKey, authenticationToken, true, false, "http://storage-balancer.realtime.co/server/1.0", googleProjectId, androidApplicationContext);
	}
	
	/**
	 * Creates a new Storage reference.
	 * 
	 * @param applicationKey 
	 * 		The application key
	 * @param privateKey
	 * 		The private key
	 * @param authenticationToken
	 * 		The authentication token.
	 * @param isCluster
	 * 		Specifies if url is cluster.
	 * @param isSecure
	 * 		Defines if connection use ssl.
	 * @param url
	 * 		The url of the storage server.
	 * @deprecated All private key references will be removed in the upcoming library versions. Private key is meant only for server-side use.
	 * Use {@link StorageRef(String applicationKey, String authenticationToken, boolean isCluster, boolean isSecure, String url)} instead.
	 *  
	 */
	@Deprecated
	public StorageRef(String applicationKey, String privateKey, String authenticationToken, boolean isCluster, boolean isSecure, String url) throws StorageException{
		context = new StorageContext(this, applicationKey, privateKey, authenticationToken, isCluster, isSecure, url, null, null,null);
	}
	
	/**
     * Creates a new Storage reference.
     * 
     * @param applicationKey 
     *    The application key
     * @param privateKey
     *    The private key
     * @param authenticationToken
     *    The authentication token.
     * @param isCluster
     *    Specifies if url is cluster.
     * @param isSecure
     *    Defines if connection use ssl.
     * @param url
     *    The url of the storage server.
     * @param googleProjectId
     *    Your google project id.
     * @param androidApplicationContext
     * Your android application context.
 	 * @deprecated All private key references will be removed in the upcoming library versions. Private key is meant only for server-side use.
	 * Use {@link StorageRef(String applicationKey, String authenticationToken, boolean isCluster, boolean isSecure, String url, String googleProjectId, android.content.Context androidApplicationContext)} instead.
	 */
	@Deprecated
	public StorageRef(String applicationKey, String privateKey, String authenticationToken, boolean isCluster, boolean isSecure, String url, String googleProjectId, Context androidApplicationContext) throws StorageException{
		context = new StorageContext(this, applicationKey, privateKey, authenticationToken, isCluster, isSecure, url, null, googleProjectId, androidApplicationContext);
	}
	
	/**
	 * Creates a new Storage reference.
	 * 
	 * @param applicationKey 
	 * 		Public key of the application's license.
	 * @param authenticationToken
	 * 		An authenticated token.
	 */
	public StorageRef(String applicationKey, String authenticationToken) throws StorageException {
		this(applicationKey, authenticationToken, true, false, "http://storage-balancer.realtime.co/server/1.0");
	}
	
	/**
   * Creates a new Storage reference.
   * 
   * @param applicationKey 
   *    Public key of the application's license.
   * @param authenticationToken
   *    An authenticated token.
   * @param googleProjectId
   *    Your google project id.
   * @param androidApplicationContext
   *    Your android application context.
   */
	public StorageRef(String applicationKey, String authenticationToken, String googleProjectId, Context androidApplicationContext) throws StorageException {
		this(applicationKey, authenticationToken, true, false, "http://storage-balancer.realtime.co/server/1.0", null, googleProjectId, androidApplicationContext);
	}
	
	/**
	 * Creates a new Storage reference.
	 * 
	 * @param applicationKey 
	 * 		Public key of the application's license.
	 * @param authenticationToken
	 * 		An authenticated token.
	 * @param isCluster
	 * 		Defines if the specified url is from a cluster server. Defaults to true.
	 * @param isSecure
	 * 		Defines if the requests and notifications are under a secure connection. Defaults to true.
	 * @param url
	 * 		The url of the storage server. Defaults to "storage.realtime.co".
	 *  
	 */
	public StorageRef(String applicationKey, String authenticationToken, boolean isCluster, boolean isSecure, String url) throws StorageException {
		context = new StorageContext(this, applicationKey, null, authenticationToken, isCluster, isSecure, url, null, null,null);
	}
	
	/**
   * Creates a new Storage reference.
   * 
   * @param applicationKey 
   *    Public key of the application's license.
   * @param authenticationToken
   *    An authenticated token.
   * @param isCluster
   *    Defines if the specified url is from a cluster server. Defaults to true.
   * @param isSecure
   *    Defines if the requests and notifications are under a secure connection. Defaults to true.
   * @param url
   *    The url of the storage server. Defaults to "storage.realtime.co".
   * @param heartbeat
   *    Configuration of the heartbeat feature. It's a small message periodically sent to the Realtime Messaging servers. When the Realtime Messaging Server detects that a given client isn't sending the heartbeat for a specified period (the default is 3 lost heartbeats) it will consider that the user has disconnected and perform the normal disconnection operations (e.g. decrease the number of subscribers). An heartbeat configuration error doesn't prevent storage creation.
   * @param googleProjectId
   *    Your google project id.
   * @param androidApplicationContext
   *    Your android application context.
   */
	public StorageRef(String applicationKey, String authenticationToken, boolean isCluster, boolean isSecure, String url, Heartbeat heartbeat, String googleProjectId, Context androidApplicationContext) throws StorageException {
		context = new StorageContext(this, applicationKey, null, authenticationToken, isCluster, isSecure, url, heartbeat, googleProjectId, androidApplicationContext);
  }

    /**
     * Event fired when a connection is established
     *
     * @return Current storage reference
     */
    public StorageRef onConnected(OnConnected onConnected){
        context.setOnConnected(onConnected,this);
        return this;
    }

	/**
	 * Event fired when a connection is reestablished after being closed unexpectedly
	 * 
	 * @return Current storage reference
	 */
	public StorageRef onReconnected(OnReconnected onReconnected) {
		context.setOnReconnected(onReconnected, this);
		return this;
	}
	
	/**
	 * Event fired when a connection is trying to be reestablished after being closed unexpectedly
	 * 
	 * @return Current storage reference
	 */
	public StorageRef onReconnecting(OnReconnecting onReconnecting) {
		context.setOnReconnecting(onReconnecting, this);
		return this;
	}
	
	/**
	 * Activate offline buffering, which buffers item’s modifications and applies them when connection reestablish. The offline buffering is activated by default.
	 * 
	 * @return Current storage reference
	 */
	public StorageRef activateOfflineBuffering() {
		context.bufferIsActive = true;
		return this;
	}
	
	/**
	 * Deactivate offline buffering, which buffers item’s modifications and applies them when connection reestablish. The offline buffering is activated by default.
	 * 
	 * @return Current storage reference
	 */
	public StorageRef deactivateOfflineBuffering() {
		context.bufferIsActive = false;
		return this;
	}
	
	/**
	 * Retrieves a list of the names of all tables created by the user's subscription.
	 * 
	 * <pre>
	 * StorageRef storage = new StorageRef("your_app_key", "your_token");
	 * 
	 * storage.getTables(new OnTableSnapshot() {
     *       &#064;Override
     *       public void run(TableSnapshot tableSnapshot) {
     *           if(tableSnapshot != null) {
     *               Log.d("StorageRef", "Table Name: " + tableSnapshot.val());
     *           }
     *       }
     *   },new OnError() {
     *       &#064;Override
     *       public void run(Integer integer, String errorMessage) {
     *           Log.e("StorageRef","Error retrieving tables: " + errorMessage);
     *       }
     *   });
     * 
     * </pre>
	 * @param onTableSnapshot
	 * 		The callback to call once the values are available. The function will be called with a table snapshot as argument, as many times as the number of tables existent. In the end, when all calls are done, the success function will be called with null as argument to signal that there are no more tables.
	 * @param onError
	 * 		The callback to call if an exception occurred
	 * @return Current storage reference
	 */
	public StorageRef getTables(OnTableSnapshot onTableSnapshot, OnError onError){
		PostBodyBuilder pbb = new PostBodyBuilder(context);		
		Rest r = new Rest(context, RestType.LISTTABLES, pbb, null);
		r.onError = onError;
		r.onTableSnapshot = onTableSnapshot;
		context.processRest(r);	
		return this;
	}
	
	/**
	 * Checks if a specified authentication token is authenticated.
	 * 
	 * <pre>
	 * StorageRef storage = new StorageRef("your_app_key", "your_token");
	 * 
	 * storage.isAuthenticated("authToken",new OnBooleanResponse() {
     *       &#064;Override
     *       public void run(Boolean aBoolean) {
     *           Log.d("StorageRef", "Is Authenticated? : " + aBoolean);
     *       }
     *   }, new OnError() {
     *       &#064;Override
     *       public void run(Integer integer, String errorMessage) {
     *           Log.e("StorageRef","Error checking authentication: " + errorMessage);
     *       }
     *   });
	 * </pre>
	 * 
	 * @param authenticationToken
	 * 		The token to verify.
	 * @param onBooleanResponse
	 * 		The callback to call when the operation is completed, with an argument as a result of verification.
	 * @param onError
	 * 		The callback to call if an exception occurred
	 * @return Current storage reference
	 */
	public StorageRef isAuthenticated(String authenticationToken, OnBooleanResponse onBooleanResponse, OnError onError){
		PostBodyBuilder pbb = new PostBodyBuilder(context);
		Rest r = new Rest(context, RestType.ISAUTHENTICATED, pbb, null);
		r.onError = onError;
		r.onBooleanResponse = onBooleanResponse;
		r.rawBody = "{\"applicationKey\":\""+context.applicationKey+"\", \"authenticationToken\":\""+authenticationToken+"\"}";
		context.processRest(r);
		return this;		
	}
	
	/**
	 * Creates new table reference
	 * 
	 * <pre>
	 * TableRef tableRef = storage.table("your_table");
	 * </pre>
	 * @param name
	 * 		The table name
	 * 
	 * @return The table reference
	 */
	public TableRef table(String name){
		return new TableRef(context, name);
	}
	
	public StorageRef setHeartbeat(Heartbeat heartbeat, OnError onError) {
		context.setHeartbeat(heartbeat);
		return this;
	}
	
	public StorageRef getHeartbeat(OnHeartbeat onHeartbeat, OnError onError) {
		try {
			onHeartbeat.run(context.getHeartbeat());
		}
		catch (StorageException e) {
			onError.run(1012, e.getMessage());
		}
		
		return this;
	}
}
