package co.realtime.storage;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import co.realtime.storage.Filter.StorageFilter;
import co.realtime.storage.Rest.RestType;
import co.realtime.storage.StorageRef.StorageDataType;
import co.realtime.storage.StorageRef.StorageEvent;
import co.realtime.storage.StorageRef.StorageOrder;
import co.realtime.storage.StorageRef.StorageProvisionLoad;
import co.realtime.storage.StorageRef.StorageProvisionType;
import co.realtime.storage.entities.Key;
import co.realtime.storage.entities.TableMetadata;
import co.realtime.storage.entities.Throughput;
import co.realtime.storage.ext.OnBooleanResponse;
import co.realtime.storage.ext.OnError;
import co.realtime.storage.ext.OnItemSnapshot;
import co.realtime.storage.ext.OnPresence;
import co.realtime.storage.ext.OnTableCreation;
import co.realtime.storage.ext.OnTableMetadata;
import co.realtime.storage.ext.OnTableUpdate;

public class TableRef {
	StorageContext context;
	String name;
	Long limit;
	StorageOrder order;
	Set<Filter> filters;
	String channel;
	protected Boolean pushNotificationsEnabled;
	private LinkedHashMap<String, Object> key;


	TableRef(StorageContext context, String name) {
		this.context = context;
		this.name = name;
		this.limit = null;
		this.order = StorageOrder.NULL;
		this.filters = new HashSet<Filter>();
		this.pushNotificationsEnabled = false;
		this.channel = "rtcs_" + this.name;
	}

	
	/**
	 * Adds a new table with primary key to the user’s application. Take into account that, even though this operation completes, the table stays in a ‘creating’ state. While in this state, all operations done over this table will fail with a ResourceInUseException.
	 * 
	 * <pre>
	 * StorageRef storage = new StorageRef("your_app_key", "your_token");
	 * 
	 * TableRef tableRef = storage.table("your_table");
	 * 
	 * // Create table "your_table" with the following schema
	 * tableRef.create("id",StorageRef.StorageDataType.STRING,
     *           StorageRef.StorageProvisionType.LIGHT, StorageRef.StorageProvisionLoad.BALANCED,new OnTableCreation() {
     *               &#064;Override
     *               public void run(String table, Double creationDate, String status) {
     *                   Log.d("TableRef", "Table with name: " + table + ",  created at: " + new Date((long)(creationDate*1000.0)).toString() + ", with status: "+status);
     *               }
     *           },new OnError() {
     *               &#064;Override
     *               public void run(Integer code, String errorMessage) {
     *                   Log.e("TableRef", "Error creating table: " + errorMessage);
     *               }
     *           });
	 * </pre>
	 * 
	 * @param primaryKeyName
	 * 		The primary key
	 * @param primaryKeyDataType
	 * 		The primary key data type
	 * @param provisionType
	 * 		The provision type
	 * @param provisionLoad
	 * 		The provision load
	 * @param onTableCreation
	 * 		The callback to call when the operation is completed 
	 * @param onError
	 * 		The callback to call if an exception occurred
	 * @return Table reference
	 */
	public TableRef create(String primaryKeyName, StorageDataType primaryKeyDataType, StorageProvisionType provisionType,
			StorageProvisionLoad provisionLoad, OnTableCreation onTableCreation, OnError onError){
		
		create(primaryKeyName, primaryKeyDataType, null, null, provisionType, provisionLoad, onTableCreation, onError);
		return this;
	}

	/**
	 * Adds a new table with primary and secondary keys to the user’s application. Take into account that, even though this operation completes, the table stays in a ‘creating’ state. While in this state, all operations done over this table will fail with a ResourceInUseException.
	 * 
	 *  <pre>
	 * StorageRef storage = new StorageRef("your_app_key", "your_token");
	 * 
	 * TableRef tableRef = storage.table("your_table");
	 * 
	 * // Create table "your_table" with the following schema
	 * tableRef.create("id",StorageRef.StorageDataType.STRING,"timestamp", StorageRef.StorageDataType.NUMBER,
     *           StorageRef.StorageProvisionType.LIGHT, StorageRef.StorageProvisionLoad.BALANCED,new OnTableCreation() {
     *               &#064;Override
     *               public void run(String table, Double creationDate, String status) {
     *                   Log.d("TableRef", "Table with name: " + table + ",  created at: " + new Date((long)(creationDate*1000.0)).toString() + ", with status: "+status);
     *               }
     *           },new OnError() {
     *               &#064;Override
     *               public void run(Integer code, String errorMessage) {
     *                   Log.e("TableRef", "Error creating table: " + errorMessage);
     *               }
     *           });
	 * </pre>
	 * 
	 * @param primaryKeyName
	 * 		The primary key
	 * @param primaryKeyDataType
	 * 		The primary key data type
	 * @param secondaryKeyName
	 * 		The secondary key
	 * @param secondaryKeyDataType
	 * 		The secondary key data type
	 * @param provisionType
	 * 		The provision type
	 * @param provisionLoad
	 * 		The provision load
	 * @param onTableCreation
	 * 		The callback to call when the operation is completed
	 * @param onError
	 * 		The callback to call if an exception occurred
	 * @return Table reference
	 */
	public TableRef create(String primaryKeyName, StorageDataType primaryKeyDataType, String secondaryKeyName, 
			StorageDataType secondaryKeyDataType, StorageProvisionType provisionType,
			StorageProvisionLoad provisionLoad, OnTableCreation onTableCreation, OnError onError){
		
		PostBodyBuilder pbb = new PostBodyBuilder(context);
		pbb.addObject("table", this.name);
		pbb.addObject("provisionLoad", provisionLoad.getValue());
		pbb.addObject("provisionType", provisionType.getValue());
		Map <String, Object> key = new HashMap<String, Object>();
		Map <String, Object> primary = new HashMap<String, Object>();
		primary.put("name", primaryKeyName);
		primary.put("dataType", primaryKeyDataType.toString());
		key.put("primary", primary);
		if(secondaryKeyName != null && secondaryKeyDataType != null){
			Map <String, Object> secondary = new HashMap<String, Object>();
			secondary.put("name", secondaryKeyName);
			secondary.put("dataType", secondaryKeyDataType.toString());
			key.put("secondary", secondary);
		}			
		pbb.addObject("key", key);
		Rest r = new Rest(context, RestType.CREATETABLE, pbb, null);
		r.onError = onError;
		r.onTableCreation = onTableCreation;
		context.processRest(r);
		return this;
	}
	
	/**
	 * Creates a table with a custom throughput. The provision type is Custom and the provision load is ignored.
	 * 
	 * <pre>
	 * StorageRef storage = new StorageRef("your_app_key", "your_token");
	 * 
	 * TableRef tableRef = storage.table("your_table");
	 * 
	 * // Create table 'myTable' with the following schema (with custom provisioning)
	 * tableRef.create(new Key(new KeySchema("id", StorageRef.StorageDataType.STRING),new KeySchema("timestamp", StorageRef.StorageDataType.NUMBER)),
     *           new Throughput(1,1),new OnTableCreation() {
     *               &#064;Override
     *               public void run(String table, Double creationDate, String status) {
     *                   Log.d("TableRef", "Table with name: " + table + ",  created at: " + new Date((long)(creationDate*1000.0)).toString() + ", with status: "+status);
     *               }
     *           },new OnError() {
     *               &#064;Override
     *               public void run(Integer code, String errorMessage) {
     *                   Log.e("TableRef", "Error creating table: " + errorMessage);
     *               }
     *           });
	 * 
	 * </pre>
	 * 
	 * @param key The schema of the primary and secondary (optional) keys.
	 * @param throughput The number of read and write operations per second.
	 * @param onTableCreation
	 * @param onError
	 * @return Table reference
	 */
	public TableRef create(Key key, Throughput throughput, OnTableCreation onTableCreation, OnError onError) {
		PostBodyBuilder pbb = new PostBodyBuilder(context);
		pbb.addObject("table", this.name);
		pbb.addObject("provisionType", StorageProvisionType.CUSTOM.getValue());
		pbb.addObject("key", key.map());
		pbb.addObject("throughput", throughput.map());
		Rest r = new Rest(context, RestType.CREATETABLE, pbb, null);
		r.onError = onError;
		r.onTableCreation = onTableCreation;
		context.processRest(r);
		
		return this;		
	}
	
	/**
	 * Creates a table with a 
	 * 
	 * <pre>
	 * StorageRef storage = new StorageRef("your_app_key", "your_token");
	 * 
	 * TableRef tableRef = storage.table("your_table");
	 * 
	 * // Create table "your_table" with the following schema
	 * tableRef.create(new Key(new KeySchema("id", StorageRef.StorageDataType.STRING),new KeySchema("timestamp", StorageRef.StorageDataType.NUMBER)),
     *           StorageRef.StorageProvisionType.LIGHT, StorageRef.StorageProvisionLoad.BALANCED,new OnTableCreation() {
     *               &#064;Override
     *               public void run(String table, Double creationDate, String status) {
     *                   Log.d("TableRef", "Table with name: " + table + ", created at: " + new Date((long)(creationDate*1000.0)).toString() + ", with status: "+status);
     *               }
     *           },new OnError() {
     *               &#064;Override
     *               public void run(Integer code, String errorMessage) {
     *                   Log.e("TableRef", "Error creating table: " + errorMessage);
     *               }
     *           });
	 * </pre>
	 * 
	 * @param key The schema of the primary and secondary (optional) keys.
	 * @param provisionType
	 * @param provisionLoad
	 * @param onTableCreation
	 * @param onError
	 * @return Table reference
	 */
	public TableRef create(Key key, StorageProvisionType provisionType, StorageProvisionLoad provisionLoad, OnTableCreation onTableCreation, OnError onError) {
		PostBodyBuilder pbb = new PostBodyBuilder(context);
		pbb.addObject("table", this.name);
		pbb.addObject("provisionLoad", provisionLoad.getValue());
		pbb.addObject("provisionType", provisionType.getValue());
		pbb.addObject("key", key.map());
		Rest r = new Rest(context, RestType.CREATETABLE, pbb, null);
		r.onError = onError;
		r.onTableCreation = onTableCreation;
		context.processRest(r);
		
		return this;		
	}
	
	/**
	 * Delete this table.
	 * 
	 * <pre>
	 * StorageRef storage = new StorageRef("your_app_key", "your_token");
	 * 
	 * TableRef tableRef = storage.table("your_table");
	 * 
	 * // Delete table 'your_table'
	 * tableRef.del(new OnBooleanResponse() {
     *       &#064;Override
     *       public void run(Boolean aBoolean) {
     *           Log.d("TableRef", "Deleted? : " + aBoolean);
     *       }
     *   },new OnError() {
     *       &#064;Override
     *       public void run(Integer integer, String errorMessage) {
     *           Log.e("TableRef", "Error deleting table: " + errorMessage);
     *       }
     *   });
	 * </pre>
	 * 
	 * @param onBooleanResponse
	 * 		The callback to run once the table is deleted
	 * @param onError
	 * 		The callback to call if an exception occurred
	 */
	public void del(OnBooleanResponse onBooleanResponse, OnError onError){
		PostBodyBuilder pbb = new PostBodyBuilder(context);		
		pbb.addObject("table", this.name);
		Rest r = new Rest(context, RestType.DELETETABLE, pbb, null);
		r.onError = onError;
		r.onBooleanResponse = onBooleanResponse;
		context.processRest(r);			
	}
	
	/**
	 * Gets the metadata of the table reference.
	 * 
	 * <pre>
	 * StorageRef storage = new StorageRef("your_app_key", "your_token");
	 * 
	 * TableRef tableRef = storage.table("your_table");
	 * 
	 * tableRef.meta(new OnTableMetadata() {
     *       &#064;Override
     *       public void run(TableMetadata tableMetadata) {
     *           if(tableMetadata != null){
     *               Log.d("TableRef", "TableMetadata: " + tableMetadata.toString());
     *           }
     *       }
     *   }, new OnError() {
     *       &#064;Override
     *       public void run(Integer integer, String errorMessage) {
     *           Log.e("TableRef", "Error retrieving table metadata : " + errorMessage);
     *       }
     *   });
     * 
     * </pre>
	 * 
	 * @param onTableMetadata
	 * 		The callback to run once the metadata is retrieved
	 * @param onError
	 * 		The callback to call if an exception occurred
	 * @return Current table reference
	 */
	public TableRef meta(OnTableMetadata onTableMetadata, OnError onError){
		PostBodyBuilder pbb = new PostBodyBuilder(context);		
		pbb.addObject("table", this.name);
		Rest r = new Rest(context, RestType.DESCRIBETABLE, pbb, null);
		r.onError = onError;
		r.onTableMetadata = onTableMetadata;
		context.processRest(r);
		return this;		
	}
	
	/**
	 * Return the name of the referred table.
	 * 
	 *  <pre>
	 * StorageRef storage = new StorageRef("your_app_key", "your_token");
	 * 
	 * TableRef tableRef = storage.table("your_table");
	 * 
	 * Log.d("TableRef", "Table name: " + tableRef.name());
     * 
     * </pre>
     * 
	 * @return The name of the table
	 */
	public String name(){
		return this.name;	
	}
	
	/**
	 * Define if the items will be retrieved in ascendent order.
	 * 
	 *  <pre>
	 * StorageRef storage = new StorageRef("your_app_key", "your_token");
	 * 
	 * TableRef tableRef = storage.table("your_table");
	 * 
	 * tableRef.asc().getItems(new OnItemSnapshot() {
     *       &#064;Override
     *       public void run(ItemSnapshot itemSnapshot) {
     *           if(itemSnapshot != null){
     *               Log.d("TableRef", "Item retrieved: " + itemSnapshot.val());
     *           }
     *       }
     *   },new OnError() {
     *       &#064;Override
     *       public void run(Integer code, String errorMessage) {
     *           Log.e("TableRef","Error retrieving items: " + errorMessage);
     *       }
     *   });
     * 
     * </pre>
     * 
	 * @return Current table reference
	 */
	public TableRef asc(){
		this.order = StorageOrder.ASC;
		return this;
	}
	
	/**
	 * Define if the items will be retrieved in descendant order.
	 * 
	 * <pre>
	 * StorageRef storage = new StorageRef("your_app_key", "your_token");
	 * 
	 * TableRef tableRef = tableRef = storage.table("your_table");
	 * 
	 * tableRef.desc().getItems(new OnItemSnapshot() {
     *       &#064;Override
     *       public void run(ItemSnapshot itemSnapshot) {
     *           if(itemSnapshot != null){
     *               Log.d("TableRef", "Item retrieved: " + itemSnapshot.val());
     *           }
     *       }
     *   },new OnError() {
     *       &#064;Override
     *       public void run(Integer code, String errorMessage) {
     *           Log.e("TableRef","Error retrieving items: " + errorMessage);
     *       }
     *   });
	 * 
	 * </pre>
	 * 
	 * @return Current table reference
	 */
	public TableRef desc(){
		this.order = StorageOrder.DESC;
		return this;
	}
	
	/**
	 * Adds a new item to the table.
	 * 
	 *  <pre>
	 * StorageRef storage = new StorageRef("your_app_key", "your_token");
	 * 
	 * TableRef tableRef = storage.table("your_table");
	 * 
	 * LinkedHashMap lhm = new LinkedHashMap();
     *   // Put elements to the map
     *   lhm.put("your_primary_key","new_primary_key_value");
     *   lhm.put("your_secondary_key", "new_secondary_key_value");
     *   lhm.put("itemProperty", "new_itemproperty_value");
     *   tableRef.push(lhm,new OnItemSnapshot() {
     *       &#064;Override
     *       public void run(ItemSnapshot itemSnapshot) {
     *           if (itemSnapshot != null) {
     *               Log.d("TableRef", "Item inserted: " + itemSnapshot.val());
     *           }
     *       }
     *   }, new OnError() {
     *       &#064;Override
     *       public void run(Integer integer, String errorMessage) {
     *           Log.e("TableRef", "Error inserting item: " + errorMessage);
     *       }
     *   });
	 * 
	 * </pre>
	 * 
	 * @param item
	 * 		The item to add
	 * @param onItemSnapshot
	 * 		The callback to run once the insertion is done.
	 * @param onError
	 * 		The callback to call if an exception occurred
	 * @return Current table reference
	 */
	public TableRef push(LinkedHashMap<String, ItemAttribute> item, OnItemSnapshot onItemSnapshot, OnError onError){
		PostBodyBuilder pbb = new PostBodyBuilder(context);
		pbb.addObject("table", this.name);
		pbb.addObject("item", item);
		Rest r = new Rest(context, RestType.PUTITEM, pbb, this);
		r.onError = onError;
		r.onItemSnapshot = onItemSnapshot;
		context.processRest(r);
		return this;
	}
	
	/**
	 * Applies a limit to this reference confining the number of items.
	 * 
	 * <pre>
	 * StorageRef storage = new StorageRef("your_app_key", "your_token");
	 * 
	 * TableRef tableRef = storage.table("your_table");
	 * 
	 * // Retrieves, up to 10, items from table "your_table"
	 * tableRef.limit((long) 10).getItems(new OnItemSnapshot() {
     *       &#064;Override
     *       public void run(ItemSnapshot itemSnapshot) {
     *           if (itemSnapshot != null) {
     *               Log.d("TableRef", "Item retrieved: " + itemSnapshot.val());
     *           }
     *       }
     *   }, new OnError() {
     *       &#064;Override
     *       public void run(Integer code, String errorMessage) {
     *           Log.e("TableRef", "Error retrieving items: " + errorMessage);
     *       }
     *   });
	 * 
	 * </pre>
	 * 
	 * @param value
	 * 		The limit to apply.
	 * @return Current table reference
	 */
	public TableRef limit(Long value){
		this.limit = value;
		return this;
	}
	
	private final void _update(StorageProvisionLoad provisionLoad, StorageProvisionType provisionType, OnTableUpdate onTableUpdate, OnError onError){
		TableMetadata tm = context.getTableMeta(this.name);
		if((Math.abs(tm.getProvisionLoad().value - provisionLoad.value) + Math.abs(tm.getProvisionType().value - provisionType.value)) > 1){
			if(onError != null){
				onError.run(1001, "You can not make such a radical change to throughput");
			}
			return;
		}		
		PostBodyBuilder pbb = new PostBodyBuilder(context);		
		pbb.addObject("table", this.name);
		pbb.addObject("provisionLoad", provisionLoad.getValue());
		pbb.addObject("provisionType", provisionType.getValue());
		Rest r = new Rest(context, RestType.UPDATETABLE, pbb, null);
		r.onError = onError;
		r.onTableUpdate = onTableUpdate;
		context.processRest(r);
	}

	/**
	 * Updates the provision type and provision load of the referenced table.
	 * 
	 *  <pre>
	 * StorageRef storage = new StorageRef("your_app_key", "your_token");
	 * 
	 * TableRef tableRef = storage.table("your_table");
	 * 
     *   //change ProvisionType
     *   //Note: you can't change ProvisionType and ProvisionLoad at the same time
     *   tableRef.update(StorageRef.StorageProvisionLoad.READ, StorageRef.StorageProvisionType.MEDIUM,new OnTableUpdate() {
     *       &#064;Override
     *       public void run(String tableName, String status) {
     *           Log.d("TableRef", "Table : " + tableName + ", status : "+status);
     *       }
     *   }, new OnError() {
     *       &#064;Override
     *       public void run(Integer integer, String errorMessage) {
     *           Log.e("TableRef", "Error updating table: " + errorMessage);
     *       }
     *   });
	 * 
	 * </pre>
	 * 
	 * @param provisionLoad
	 * 		The new provision load
	 * @param provisionType
	 * 		The new provision type
	 * @param onTableUpdate
	 * 		The callback to run once the table is updated
	 * @param onError
	 * 		The callback to call if an exception occurred
	 * @return Current table reference
	 */
	public TableRef update(final StorageProvisionLoad provisionLoad, final StorageProvisionType provisionType, final OnTableUpdate onTableUpdate, final OnError onError){
		TableMetadata tm = context.getTableMeta(this.name);
		if(tm == null){
			this.meta(new OnTableMetadata(){
				@Override
				public void run(TableMetadata tableMetadata) {
					_update(provisionLoad, provisionType, onTableUpdate, onError);
				}				
			}, onError);
		} else {
			this._update(provisionLoad, provisionType, onTableUpdate, onError);
		}
		return this;
	}
	
	/**
	 * Applies a filter to the table. When fetched, it will return the items that match the filter property value.
	 * 
	 * <pre>
	 * StorageRef storage = new StorageRef("your_app_key", "your_token");
	 * 
	 * TableRef tableRef = storage.table("your_table");
	 * 
	 * // Retrieve all items that have their "itemProperty" value equal to "theValue"
	 * tableRef.equals("itemProperty",new ItemAttribute("theValue")).getItems(new OnItemSnapshot() {
     *       &#064;Override
     *       public void run(ItemSnapshot itemSnapshot) {
     *           if (itemSnapshot != null) {
     *               Log.d("TableRef", "Item retrieved: " + itemSnapshot.val());
     *           }
     *       }
     *   }, new OnError() {
     *       &#064;Override
     *       public void run(Integer code, String errorMessage) {
     *           Log.e("TableRef", "Error retrieving items: " + errorMessage);
     *       }
     *   });
	 * 
	 * </pre>
	 * 
	 * @param attributeName
	 * 		The name of the property to filter.
	 * @param value
	 * 		The value of the property to filter.
	 * @return Current table reference
	 */
	public TableRef equals(String attributeName, ItemAttribute value){
		filters.add(new Filter(StorageFilter.EQUALS, attributeName, value, null));
		return this;
	}
	/**
	 * Applies a filter to the table. When fetched, it will return the items that does not match the filter property value.
	 * 
	 * <pre>
	 * StorageRef storage = new StorageRef("your_app_key", "your_token");
	 * 
	 * TableRef tableRef = storage.table("your_table");
	 * 
	 * // Retrieve all items that have their "itemProperty" value equal to "theValue"
	 * tableRef.notEqual("itemProperty",new ItemAttribute("theValue")).getItems(new OnItemSnapshot() {
     *       &#064;Override
     *       public void run(ItemSnapshot itemSnapshot) {
     *           if (itemSnapshot != null) {
     *               Log.d("TableRef", "Item retrieved: " + itemSnapshot.val());
     *           }
     *       }
     *   }, new OnError() {
     *       &#064;Override
     *       public void run(Integer code, String errorMessage) {
     *           Log.e("TableRef", "Error retrieving items: " + errorMessage);
     *       }
     *   });
	 * 
	 * </pre>
	 * 
	 * @param attributeName
	 * 		The name of the property to filter.
	 * @param value
	 * 		The value of the property to filter.
	 * @return Current table reference
	 */
	public TableRef notEqual(String attributeName, ItemAttribute value){
		filters.add(new Filter(StorageFilter.NOTEQUAL, attributeName, value, null));
		return this;
	}
	/**
	 * Applies a filter to the table. When fetched, it will return the items greater or equal to filter property value.
	 * 
	 * <pre>
	 * StorageRef storage = new StorageRef("your_app_key", "your_token");
	 * 
	 * TableRef tableRef = tableRef = storage.table("your_table");
	 * 
	 * // Retrieve all items that have their "itemProperty" value greater or equal to 10
	 * tableRef.greaterEqual("itemProperty",new ItemAttribute(10)).getItems(new OnItemSnapshot() {
     *       &#064;Override
     *       public void run(ItemSnapshot itemSnapshot) {
     *           if (itemSnapshot != null) {
     *               Log.d("TableRef", "Item retrieved: " + itemSnapshot.val());
     *           }
     *       }
     *   }, new OnError() {
     *       &#064;Override
     *       public void run(Integer code, String errorMessage) {
     *           Log.e("TableRef", "Error retrieving items: " + errorMessage);
     *       }
     *   });
	 * 
	 * </pre>
	 * 
	 * @param attributeName
	 * 		The name of the property to filter.
	 * @param value
	 * 		The value of the property to filter.
	 * @return Current table reference
	 */
	public TableRef greaterEqual(String attributeName, ItemAttribute value){
		filters.add(new Filter(StorageFilter.GREATEREQUAL, attributeName, value, null));
		return this;
	}
	/**
	 * Applies a filter to the table. When fetched, it will return the items greater than the filter property value.
	 * 
	 * <pre>
	 * StorageRef storage = new StorageRef("your_app_key", "your_token");
	 * 
	 * TableRef tableRef = storage.table("your_table");
	 * 
	 * // Retrieve all items that have their "itemProperty" value greater than 10
	 * tableRef.greaterThan("itemProperty",new ItemAttribute(10)).getItems(new OnItemSnapshot() {
     *       &#064;Override
     *       public void run(ItemSnapshot itemSnapshot) {
     *           if (itemSnapshot != null) {
     *               Log.d("TableRef", "Item retrieved: " + itemSnapshot.val());
     *           }
     *       }
     *   }, new OnError() {
     *       &#064;Override
     *       public void run(Integer code, String errorMessage) {
     *           Log.e("TableRef", "Error retrieving items: " + errorMessage);
     *       }
     *   });
	 * 
	 * </pre>
	 * 
	 * @param attributeName
	 * 		The name of the property to filter.
	 * @param value
	 * 		The value of the property to filter.
	 * @return Current table reference
	 */
	public TableRef greaterThan(String attributeName, ItemAttribute value){
		filters.add(new Filter(StorageFilter.GREATERTHAN, attributeName, value, null));
		return this;
	}
	/**
	 * Applies a filter to the table. When fetched, it will return the items lesser or equals to the filter property value.
	 * 
	 * <pre>
	 * StorageRef storage = new StorageRef("your_app_key", "your_token");
	 * 
	 * TableRef tableRef = storage.table("your_table");
	 * 
	 * // Retrieve all items that have their "itemProperty" value lesser or equal 10
	 * tableRef.lessEqual("itemProperty",new ItemAttribute(10)).getItems(new OnItemSnapshot() {
     *       &#064;Override
     *       public void run(ItemSnapshot itemSnapshot) {
     *           if (itemSnapshot != null) {
     *               Log.d("TableRef", "Item retrieved: " + itemSnapshot.val());
     *           }
     *       }
     *   }, new OnError() {
     *       &#064;Override
     *       public void run(Integer code, String errorMessage) {
     *           Log.e("TableRef", "Error retrieving items: " + errorMessage);
     *       }
     *   });
	 * 
	 * </pre>
	 * 
	 * @param attributeName
	 * 		The name of the property to filter.
	 * @param value
	 * 		The value of the property to filter.
	 * @return Current table reference
	 */
	public TableRef lessEqual(String attributeName, ItemAttribute value){
		filters.add(new Filter(StorageFilter.LESSEREQUAL, attributeName, value, null));
		return this;
	}
	/**
	 * Applies a filter to the table. When fetched, it will return the items lesser than the filter property value.
	 * 
	 * <pre>
	 * StorageRef storage = new StorageRef("your_app_key", "your_token");
	 * 
	 * TableRef tableRef = storage.table("your_table");
	 * 
	 * // Retrieve all items that have their "itemProperty" value lesser than 10
	 * tableRef.lessThan("itemProperty",new ItemAttribute(10)).getItems(new OnItemSnapshot() {
     *       &#064;Override
     *       public void run(ItemSnapshot itemSnapshot) {
     *           if (itemSnapshot != null) {
     *               Log.d("TableRef", "Item retrieved: " + itemSnapshot.val());
     *           }
     *       }
     *   }, new OnError() {
     *       &#064;Override
     *       public void run(Integer code, String errorMessage) {
     *           Log.e("TableRef", "Error retrieving items: " + errorMessage);
     *       }
     *   });
	 * 
	 * </pre>
	 * 
	 * @param attributeName
	 * 		The name of the property to filter.
	 * @param value
	 * 		The value of the property to filter.
	 * @return Current table reference
	 */
	public TableRef lessThan(String attributeName, ItemAttribute value){
		filters.add(new Filter(StorageFilter.LESSERTHAN, attributeName, value, null));
		return this;
	}
	/**
	 * Applies a filter to the table reference. When fetched, it will return the non null values.
	 * 
	 * <pre>
	 * StorageRef storage = new StorageRef("your_app_key", "your_token");
	 * 
	 * TableRef tableRef = storage.table("your_table");
	 * 
	 * // Retrieve all items where their "itemProperty" value is not null
	 * tableRef.notNull("itemProperty").getItems(new OnItemSnapshot() {
     *       &#064;Override
     *       public void run(ItemSnapshot itemSnapshot) {
     *           if (itemSnapshot != null) {
     *               Log.d("TableRef", "Item retrieved: " + itemSnapshot.val());
     *           }
     *       }
     *   }, new OnError() {
     *       &#064;Override
     *       public void run(Integer code, String errorMessage) {
     *           Log.e("TableRef", "Error retrieving items: " + errorMessage);
     *       }
     *   });
	 * 
	 * </pre>
	 * 
	 * @param attributeName
	 * 		The name of the property to filter.
	 * @return Current table reference
	 */
	public TableRef notNull(String attributeName){
		filters.add(new Filter(StorageFilter.NOTNULL, attributeName, null, null));
		return this;
	}
	/**
	 * Applies a filter to the table. When fetched, it will return the null values.
	 * 
	 * <pre>
	 * StorageRef storage = new StorageRef("your_app_key", "your_token");
	 * 
	 * TableRef tableRef = storage.table("your_table");
	 * 
	 * // Retrieve all items where their "itemProperty" value is null
	 * tableRef.isNull("itemProperty").getItems(new OnItemSnapshot() {
     *       &#064;Override
     *       public void run(ItemSnapshot itemSnapshot) {
     *           if (itemSnapshot != null) {
     *               Log.d("TableRef", "Item retrieved: " + itemSnapshot.val());
     *           }
     *       }
     *   }, new OnError() {
     *       &#064;Override
     *       public void run(Integer code, String errorMessage) {
     *           Log.e("TableRef", "Error retrieving items: " + errorMessage);
     *       }
     *   });
	 * 
	 * </pre>
	 * 
	 * @param attributeName
	 * 		The name of the property to filter.
	 * @return Current table reference
	 */
	public TableRef isNull(String attributeName){
		filters.add(new Filter(StorageFilter.NULL, attributeName, null, null));
		return this;
	}
	/**
	 * Applies a filter to the table. When fetched, it will return the items that contains the filter property value.
	 * 
	 * <pre>
	 * StorageRef storage = new StorageRef("your_app_key", "your_token");
	 * 
	 * TableRef tableRef = storage.table("your_table");
	 * 
	 * // Retrieve all items with property "itemProperty" contains the value "xpto"
	 * tableRef.contains("itemProperty",new ItemAttribute("xpto")).getItems(new OnItemSnapshot() {
     *       &#064;Override
     *       public void run(ItemSnapshot itemSnapshot) {
     *           if (itemSnapshot != null) {
     *               Log.d("TableRef", "Item retrieved: " + itemSnapshot.val());
     *           }
     *       }
     *   }, new OnError() {
     *       &#064;Override
     *       public void run(Integer code, String errorMessage) {
     *           Log.e("TableRef", "Error retrieving items: " + errorMessage);
     *       }
     *   });
	 * </pre>
	 * 
	 * @param attributeName
	 * 		The name of the property to filter.
	 * @param value
	 * 		The value of the property to filter.
	 * @return Current table reference
	 */
	public TableRef contains(String attributeName, ItemAttribute value){
		filters.add(new Filter(StorageFilter.CONTAINS, attributeName, value, null));
		return this;
	}
	/**
	 * Applies a filter to the table. When fetched, it will return the items that does not contains the filter property value.
	 * 
	 *  <pre>
	 * StorageRef storage = new StorageRef("your_app_key", "your_token");
	 * 
	 * TableRef tableRef = storage.table("your_table");
	 * 
	 * // Retrieve all items with property "itemProperty" contains the value "xpto"
	 * tableRef.notContains("itemProperty",new ItemAttribute("xpto")).getItems(new OnItemSnapshot() {
     *       &#064;Override
     *       public void run(ItemSnapshot itemSnapshot) {
     *           if (itemSnapshot != null) {
     *               Log.d("TableRef", "Item retrieved: " + itemSnapshot.val());
     *           }
     *       }
     *   }, new OnError() {
     *       &#064;Override
     *       public void run(Integer code, String errorMessage) {
     *           Log.e("TableRef", "Error retrieving items: " + errorMessage);
     *       }
     *   });
	 * </pre>
	 * 
	 * @param attributeName
	 * 		The name of the property to filter.
	 * @param value
	 * 		The value of the property to filter.
	 * @return Current table reference
	 */
	public TableRef notContains(String attributeName, ItemAttribute value){
		filters.add(new Filter(StorageFilter.NOTCONTAINS, attributeName, value, null));
		return this;
	}
	/**
	 * Applies a filter to the table. When fetched, it will return the items that begins with the filter property value.
	 * 
	 * <pre>
	 * StorageRef storage = new StorageRef("your_app_key", "your_token");
	 * 
	 * TableRef tableRef = storage.table("your_table");
	 * 
	 * // Retrieve all items with property "itemProperty" value starting with "xpto"
	 * tableRef.beginsWith("itemProperty",new ItemAttribute("xpto")).getItems(new OnItemSnapshot() {
     *       &#064;Override
     *       public void run(ItemSnapshot itemSnapshot) {
     *           if (itemSnapshot != null) {
     *               Log.d("TableRef", "Item retrieved: " + itemSnapshot.val());
     *           }
     *       }
     *   }, new OnError() {
     *       &#064;Override
     *       public void run(Integer code, String errorMessage) {
     *           Log.e("TableRef", "Error retrieving items: " + errorMessage);
     *       }
     *   });
	 * </pre>
	 * 
	 * @param attributeName
	 * 		The name of the property to filter.
	 * @param value
	 * 		The value of the property to filter.
	 * @return Current table reference
	 */
	public TableRef beginsWith(String attributeName, ItemAttribute value){
		filters.add(new Filter(StorageFilter.BEGINSWITH, attributeName, value, null));
		return this;
	}
	/**
	 * Applies a filter to the table. When fetched, it will return the items in range of the filter property value.
	 * 
	 * <pre>
	 * StorageRef storage = new StorageRef("your_app_key", "your_token");
	 * 
	 * TableRef tableRef =  storage.table("your_table");
	 * 
	 * // Retrieve all items where property "itemProperty" has a value between 1 and 10
	 * tableRef.between("itemProperty",new ItemAttribute(1),new ItemAttribute(10)).getItems(new OnItemSnapshot() {
     *       &#064;Override
     *       public void run(ItemSnapshot itemSnapshot) {
     *           if (itemSnapshot != null) {
     *               Log.d("TableRef", "Item retrieved: " + itemSnapshot.val());
     *           }
     *       }
     *   }, new OnError() {
     *       &#064;Override
     *       public void run(Integer code, String errorMessage) {
     *           Log.e("TableRef", "Error retrieving items: " + errorMessage);
     *       }
     *   });
	 * </pre>
	 * 
	 * @param attributeName
	 * 		The name of the property to filter.
	 * @param startValue
	 * 		The value of property indicates the beginning of range.
	 * @param endValue
	 * 		The value of property indicates the end of range.
	 * @return Current table reference
	 */
	public TableRef between(String attributeName, ItemAttribute startValue, ItemAttribute endValue){
		filters.add(new Filter(StorageFilter.BETWEEN, attributeName, startValue, endValue));
		return this;
	}
	
	private void _getItems(OnItemSnapshot onItemSnapshot, OnError onError){
		TableMetadata tm = context.getTableMeta(this.name);
		
		RestType rt = _tryConstructKey(tm);
		if(rt == RestType.GETITEM){
			Iterator<Filter> itr = filters.iterator();
			Filter f = itr.next(); 
			StorageDataType primaryType = tm.getPrimaryKeyType();
			this.item(primaryType == StorageDataType.STRING ? new ItemAttribute(f.value.toString()) : f.value)._get(onItemSnapshot, onError, true);
			return;
		}
		//RestType rt = (lhm == null) ? RestType.LISTITEMS : RestType.QUERYITEMS;
		PostBodyBuilder pbb = new PostBodyBuilder(context);
		pbb.addObject("table", this.name);
		if(rt==RestType.QUERYITEMS){
			pbb.addObject("key", this.key);				
			if(this.limit != null)
				pbb.addObject("limit", this.limit);
		}
		if(filters.size()>0)
			pbb.addObject("filter", getFiltersForJSON(rt));
		Rest r = new Rest(context, rt, pbb, this);
		r.onError = onError;
		r.onItemSnapshot = onItemSnapshot;
		r.order = this.order;		
		if(this.limit != null){
			r.limit = this.limit;
		}
		context.processRest(r);

	}
		
	private Object getFiltersForJSON(RestType rt) {
		if(this.filters.size() == 1){
			Iterator<Filter> itr = filters.iterator();
			Filter f = itr.next();
			if(rt==RestType.LISTITEMS){
				ArrayList<Object> ar = new ArrayList<Object>();
				ar.add(f.prepareForJSON());
				return ar;
			} else {
				return f.prepareForJSON();				
			}
		} else {
			ArrayList<Object> ar = new ArrayList<Object>();
			for(Filter f : filters){
				ar.add(f.prepareForJSON());
			}
			return ar;
		}		
	}

	//if returns null the rest type is listItems, otherwise queryItems
	private RestType _tryConstructKey(TableMetadata tm) {
		for(Filter f : filters)
			if(f.operator==StorageFilter.NOTEQUAL || f.operator==StorageFilter.NOTNULL || f.operator==StorageFilter.NULL ||
			f.operator==StorageFilter.CONTAINS || f.operator==StorageFilter.NOTCONTAINS)
				return RestType.LISTITEMS; //because queryItems do not support notEqual, notNull, null, contains and notContains

		if(tm.getSecondaryKeyName()!=null){
			if(filters.size() == 1){
				Iterator<Filter> itr = filters.iterator();
				Filter f = itr.next(); 
				if(f.itemName.equals(tm.getPrimaryKeyName()) && f.operator == StorageFilter.EQUALS){
					this.key = new LinkedHashMap<String, Object>();
					this.key.put("primary", f.value);
					filters.clear();
					return RestType.QUERYITEMS;
				}			
			} else if (filters.size() == 2) {
				Object tValue = null;
				Filter tFilter = null;
				for(Filter f : filters){
					if(f.itemName.equals(tm.getPrimaryKeyName()) && f.operator == StorageFilter.EQUALS){
						tValue = f.value;
					}
					if(f.itemName.equals(tm.getSecondaryKeyName())){
						tFilter = f;
					}				
				}
				if(tValue!=null && tFilter!=null){
					filters.clear();
					filters.add(tFilter);
					this.key = new LinkedHashMap<String, Object>();
					this.key.put("primary", tValue);
					return RestType.QUERYITEMS;
				} else {
					return RestType.LISTITEMS;				
				}
			}
		} else {			
			if(filters.size()==1){
				Iterator<Filter> itr = filters.iterator();
				Filter f = itr.next(); 
				if(f.itemName.equals(tm.getPrimaryKeyName()) && f.operator == StorageFilter.EQUALS){
					return RestType.GETITEM;
				}
			}
		}
		return RestType.LISTITEMS;
	}

	/**
	 * Get the items of this tableRef.
	 * 
	 * <pre>
	 * StorageRef storage = new StorageRef("your_app_key", "your_token");
	 * 
	 * TableRef tableRef = storage.table("your_table");
	 * 
	 * tableRef.getItems(new OnItemSnapshot() {
     *       &#064;Override
     *       public void run(ItemSnapshot itemSnapshot) {
     *           if (itemSnapshot != null) {
     *               Log.d("TableRef", "Item retrieved: " + itemSnapshot.val());
     *           }
     *       }
     *   }, new OnError() {
     *       &#064;Override
     *       public void run(Integer code, String errorMessage) {
     *           Log.e("TableRef", "Error retrieving items: " + errorMessage);
     *       }
     *   });
	 * 
	 * </pre>
	 * 
	 * @param onItemSnapshot
	 * 		The callback to call once the items are available. The success function will be called for each existent item. The argument is an item snapshot. In the end, when all calls are done, the success function will be called with null as argument to signal that there are no more items.
	 * @param onError
	 * 		The callback to call if an exception occurred
	 * @return Current table reference
	 */
	public TableRef getItems(final OnItemSnapshot onItemSnapshot, final OnError onError){
		TableMetadata tm = context.getTableMeta(this.name);
		if(tm == null){
			this.meta(new OnTableMetadata(){
				@Override
				public void run(TableMetadata tableMetadata) {
					_getItems(onItemSnapshot, onError);
				}				
			}, onError);
		} else {
			this._getItems(onItemSnapshot, onError);
		}
		return this;
	}
	
	/**
	 * Creates a new item reference.
	 * 
	 * <pre>
	 * StorageRef storage = new StorageRef("your_app_key", "your_token");
	 * 
	 * TableRef tableRef = storage.table("your_table");
	 * 
	 * ItemRef itemRef = tableRef.item(new ItemAttribute("your_primary_key_value"));
	 * 
	 * </pre>
	 * 
	 * @param primaryKeyValue
	 * 		The primary key. Must match the table schema.
	 * @return Current table reference
	 */
	public ItemRef item(ItemAttribute primaryKeyValue){
		return new ItemRef(context, this, primaryKeyValue, null);
	}
	
	/**
	 * Creates a new item reference.
	 * 
	 * <pre>
	 * StorageRef storage = new StorageRef("your_app_key", "your_token");
	 * 
	 * TableRef tableRef = storage.table("your_table");
	 * 
	 * ItemRef itemRef = tableRef.item(new ItemAttribute("your_primary_key_value"),new ItemAttribute("your_secondary_key_value"));
	 * 
	 * </pre>
	 * 
	 * @param primaryKeyValue
	 * 		The primary key. Must match the table schema.
	 * @param secondaryKeyValue
	 * 		The secondary key. Must match the table schema.
	 * @return Current table reference
	 */
	public ItemRef item(ItemAttribute primaryKeyValue, ItemAttribute secondaryKeyValue){
		return new ItemRef(context, this, primaryKeyValue, secondaryKeyValue);
	}

	private final Boolean filterExists(StorageFilter filterType, String itemName) {
			Boolean filterExists = false;
			// see if equals filter exists over the primary key
			for(Filter filter : filters) {
				if(filter.itemName == itemName && filter.operator == filterType) {
					filterExists = true;
					break;
				}
			}	
			
			return filterExists;
	}
	
	/**
	 * Attach a listener to run every time the eventType occurs.
	 * 
	 * <pre>
	 * StorageRef storage = new StorageRef("your_app_key", "your_token");
	 * 
	 * TableRef tableRef = storage.table("your_table");
	 * 
	 * // Add an update listener
	 * tableRef.on(StorageRef.StorageEvent.UPDATE, new OnItemSnapshot() {
     *       &#064;Override
     *       public void run(ItemSnapshot itemSnapshot) {
     *           if (itemSnapshot != null) {
     *               Log.d("TableRef", "Item updated: " + itemSnapshot.val());
     *           }
     *       }
     *   }, new OnError() {
     *       &#064;Override
     *       public void run(Integer integer, String errorMessage) {
     *           Log.e("TableRef", "Error adding an update event listener: " + errorMessage);
     *       }
     *   });
	 * 
	 * </pre>
	 * 
	 * @param eventType
	 * 		The type of the event to listen. Possible values: put, update, delete
	 * @param onItemSnapshot
	 * 		The function to run whenever the event occurs. The function is called with the snapshot of affected item as argument.  If the event type is "put", it will immediately trigger a "getItems" to get the initial data and run the callback with each item snapshot as argument.  Note: If you are using GCM the value of received ItemSnapshot can be null.
	 * @param onError 
	 * 		Response if client side validation failed or if an error was returned from the server.
	 * @return Current table reference
	 */
	public TableRef on(StorageEvent eventType, final OnItemSnapshot onItemSnapshot, final OnError onError) {
		if(eventType == StorageEvent.PUT) {
			getItems(onItemSnapshot, onError);
		}
		Event ev = new Event(eventType, this.name, null, null, false, true, pushNotificationsEnabled, onItemSnapshot);
		context.addEvent(ev);
		//if(eventType.compareTo(StorageEvent.PUT)==0){
		//	this.getItems(onItemSnapshot, null);
		//}
		return this;
	}
	
	/**
	 * Attach a listener to run every time the eventType occurs.
	 * 
	 * <pre>
	 * StorageRef storage = new StorageRef("your_app_key", "your_token");
	 * 
	 * TableRef tableRef = storage.table("your_table");
	 * 
	 * // Add an update listener
	 * tableRef.on(StorageRef.StorageEvent.UPDATE, new OnItemSnapshot() {
     *       &#064;Override
     *       public void run(ItemSnapshot itemSnapshot) {
     *           if (itemSnapshot != null) {
     *               Log.d("TableRef", "Item updated: " + itemSnapshot.val());
     *           }
     *       }
     *   });
	 * 
	 * </pre>
	 * 
	 * @param eventType
	 * 		The type of the event to listen. Possible values: put, update, delete
	 * @param onItemSnapshot
	 * 		The function to run whenever the event occurs. The function is called with the snapshot of affected item as argument.  If the event type is "put", it will immediately trigger a "getItems" to get the initial data and run the callback with each item snapshot as argument.  Note: If you are using GCM the value of received ItemSnapshot can be null.
	 * @return Current table reference
	 */
	public TableRef on(StorageEvent eventType, final OnItemSnapshot onItemSnapshot) {
		return on(eventType, onItemSnapshot, null);
	}
	
	/**
	 * Attach a listener to run every time the eventType occurs for specific primary key.
	 * 
	 *  <pre>
	 * StorageRef storage = new StorageRef("your_app_key", "your_token");
	 * 
	 * TableRef tableRef = storage.table("your_table");
	 * 
	 * // Add an update listener
	 * tableRef.on(StorageRef.StorageEvent.UPDATE, new ItemAttribute("your_primary_key_value"),new OnItemSnapshot() {
     *       &#064;Override
     *       public void run(ItemSnapshot itemSnapshot) {
     *           if (itemSnapshot != null) {
     *               Log.d("TableRef", "Item updated: " + itemSnapshot.val());
     *           }
     *       }
     *   },new OnError() {
     *       &#064;Override
     *       public void run(Integer integer, String errorMessage) {
     *           Log.e("TableRef", "Error adding an update event listener: " + errorMessage);
     *       }
     *   });
	 * 
	 * </pre>
	 * @param eventType
	 * 		The type of the event to listen. Possible values: put, update, delete
	 * @param primary
	 * 		The primary key of the items to listen. The callback will run every time an item with the primary key is affected.
	 * @param onItemSnapshot
	 * 		The function to run whenever the event occurs. The function is called with the snapshot of affected item as argument.  If the event type is "put", it will immediately trigger a "getItems" to get the initial data and run the callback with each item snapshot as argument. Note: If you are using GCM the value of received ItemSnapshot can be null.
	 * @param onError 
	 * 		Response if client side validation failed or if an error was returned from the server.
	 * @return Current table reference
	 */
	public TableRef on(StorageEvent eventType, final ItemAttribute primary, final OnItemSnapshot onItemSnapshot, final OnError onError) {
		if(eventType == StorageEvent.PUT) {			
			final TableRef self = this;
			TableMetadata tm = context.getTableMeta(this.name);
			if(tm == null) {
				this.meta(new OnTableMetadata(){
					@Override
					public void run(TableMetadata tableMetadata) {
						// see if equals filter exists over the primary key
						if(!filterExists(StorageFilter.EQUALS, tableMetadata.getPrimaryKeyName())) {
							self.equals(tableMetadata.getPrimaryKeyName(), primary);
						}						
						_getItems(onItemSnapshot, onError);
					}			
				}, onError);
				
			}
			else {
				// see if equals filter exists over the primary key
				if(!filterExists(StorageFilter.EQUALS, tm.getPrimaryKeyName())) {
					equals(tm.getPrimaryKeyName(), primary);
				}
				_getItems(onItemSnapshot, onError);			
			}
		}
		
		Event ev = new Event(eventType, this.name, primary, null, false, true, pushNotificationsEnabled, onItemSnapshot);
		context.addEvent(ev);
		//if(eventType.compareTo(StorageEvent.PUT)==0){
		//	this.getItems(onItemSnapshot, null);
		//}
		return this;
	}

	/**
	 * Attach a listener to run every time the eventType occurs for specific primary key.
	 * 
	 *  <pre>
	 * StorageRef storage = new StorageRef("your_app_key", "your_token");
	 * 
	 * TableRef tableRef = storage.table("your_table");
	 * 
	 * // Add an update listener
	 * tableRef.on(StorageRef.StorageEvent.UPDATE, new ItemAttribute("your_primary_key_value"),new OnItemSnapshot() {
     *       &#064;Override
     *       public void run(ItemSnapshot itemSnapshot) {
     *           if (itemSnapshot != null) {
     *               Log.d("TableRef", "Item updated: " + itemSnapshot.val());
     *           }
     *       }
     *   });
	 * 
	 * </pre>
	 * 
	 * @param eventType
	 * 		The type of the event to listen. Possible values: put, update, delete
	 * @param primary
	 * 		The primary key of the items to listen. The callback will run every time an item with the primary key is affected.
	 * @param onItemSnapshot
	 * 		The function to run whenever the event occurs. The function is called with the snapshot of affected item as argument.  If the event type is "put", it will immediately trigger a "getItems" to get the initial data and run the callback with each item snapshot as argument. Note: If you are using GCM the value of received ItemSnapshot can be null.
	 * @return Current table reference
	 */
	public TableRef on(StorageEvent eventType, final ItemAttribute primary, final OnItemSnapshot onItemSnapshot) {
		return on(eventType, primary, onItemSnapshot, null);
	}
	
	/**
	 * Attach a listener to run only once the event type occurs.
	 * 
	 * <pre>
	 * StorageRef storage = new StorageRef("your_app_key", "your_token");
	 * 
	 * TableRef tableRef = storage.table("your_table");
	 * 
	 * // Add an update listener. Only one notification is received once the item is updated after the listener is set
	 * tableRef.once(StorageRef.StorageEvent.UPDATE,new OnItemSnapshot() {
     *       &#064;Override
     *       public void run(ItemSnapshot itemSnapshot) {
     *           if (itemSnapshot != null) {
     *               Log.d("TableRef", "Item updated: " + itemSnapshot.val());
     *           }
     *       }
     *   }, new OnError() {
     *       &#064;Override
     *       public void run(Integer integer, String errorMessage) {
     *           Log.e("TableRef", "Error adding an update event listener: " + errorMessage);
     *       }
     *   });
	 * </pre>
	 * 
	 * @param eventType
	 * 		The type of the event to listen. Possible values: put, update, delete
	 * @param onItemSnapshot
	 * 		The function to run when the event occurs. The function is called with the snapshot of affected item as argument.
	 * @param onError 
	 * 		Response if client side validation failed or if an error was returned from the server.
	 * @return Current table reference
	 */
	public TableRef once(StorageEvent eventType, final OnItemSnapshot onItemSnapshot, final OnError onError) {
		if(eventType == StorageEvent.PUT) {
			getItems(onItemSnapshot, onError);
		}
		Event ev = new Event(eventType, this.name, null, null, true, true, pushNotificationsEnabled, onItemSnapshot);
		context.addEvent(ev);
		return this;
	}

	/**
	 * Attach a listener to run only once the event type occurs for specific primary key.
	 * 
	 * <pre>
	 * StorageRef storage = new StorageRef("your_app_key", "your_token");
	 * 
	 * TableRef tableRef = storage.table("your_table");
	 * 
	 * // Add an update listener. Only one notification is received once the item is updated after the listener is set
	 * tableRef.once(StorageRef.StorageEvent.UPDATE, new ItemAttribute("your_primary_key_value"),new OnItemSnapshot() {
     *       &#064;Override
     *       public void run(ItemSnapshot itemSnapshot) {
     *           if (itemSnapshot != null) {
     *               Log.d("TableRef", "Item updated: " + itemSnapshot.val());
     *           }
     *       }
     *   }, new OnError() {
     *       &#064;Override
     *       public void run(Integer integer, String errorMessage) {
     *           Log.e("TableRef", "Error adding an update event listener: " + errorMessage);
     *       }
     *   });
	 * </pre>
	 * 
	 * @param eventType
	 * 		The type of the event to listen. Possible values: put, update, delete
	 * @param primary
	 * 		The primary key of the items to listen. The callback will run when item with the primary key is affected.
	 * @param onItemSnapshot
	 * 		The function to run when the event occurs. The function is called with the snapshot of affected item as argument.
	 * @param onError 
	 * 		Response if client side validation failed or if an error was returned from the server.
	 * @return Current table reference
	 */
	public TableRef once(StorageEvent eventType, final ItemAttribute primary, final OnItemSnapshot onItemSnapshot, final OnError onError) {
		if(eventType == StorageEvent.PUT) {
			final TableRef self = this;
			TableMetadata tm = context.getTableMeta(this.name);
			if(tm == null) {
				this.meta(new OnTableMetadata() {
					@Override
					public void run(TableMetadata tableMetadata) {
						// see if equals filter exists over the primary key
						if(!filterExists(StorageFilter.EQUALS, tableMetadata.getPrimaryKeyName())) {
							self.equals(tableMetadata.getPrimaryKeyName(), primary);
						}
						_getItems(onItemSnapshot, onError);
					}
				}, onError);
				
			}
			else {
				// see if equals filter exists over the primary key
				if(!filterExists(StorageFilter.EQUALS, tm.getPrimaryKeyName())) {
					equals(tm.getPrimaryKeyName(), primary);
				}
				_getItems(onItemSnapshot, onError);			
			}
		}
		
		Event ev = new Event(eventType, this.name, primary, null, true, true, pushNotificationsEnabled, onItemSnapshot);
		context.addEvent(ev);
		return this;
	}

	/**
	 * Remove an event handler.
	 * 
	 * <pre>
	 * 
	 * StorageRef storage = new StorageRef("your_app_key", "your_token");
	 * 
	 * TableRef tableRef = storage.table("your_table");
	 * 
	 * //Define handler function
	 * final OnItemSnapshot itemSnapshot = new OnItemSnapshot() {
     *       &#064;Override
     *       public void run(ItemSnapshot itemSnapshot) {
     *           if (itemSnapshot != null) {
     *               Log.d("TableRef", "Item : " + itemSnapshot.val());
     *           }
     *       }
     *   };
     *   
     *   // Add an update listener
     *   tableRef.on(StorageRef.StorageEvent.UPDATE,itemSnapshot,new OnError() {
     *       &#064;Override
     *       public void run(Integer integer, String errorMessage) {
     *           Log.e("TableRef", "Error adding an update event listener: " + errorMessage);
     *       }
     *   });
     *
     *   Handler handler=new Handler();
     *
     *   final Runnable r = new Runnable()
     *   {
     *       public void run()
     *       {	 //remove the update listener
     *           tableRef.off(StorageRef.StorageEvent.UPDATE,itemSnapshot);
     *       }
     *   };
     *	 
     *   // Stop listening after 5 seconds
     *   handler.postDelayed(r, 5000);
	 * 
	 * </pre>
	 * 
	 * @param eventType
	 * 		The type of the event to listen. Possible values: put, update, delete
	 * @param onItemSnapshot
	 * 		The callback previously attached.
	 * @return Current table reference
	 */
	public TableRef off(StorageEvent eventType, OnItemSnapshot onItemSnapshot) {
		Event ev = new Event(eventType, this.name, null, null, false, true, false, onItemSnapshot);
		context.removeEvent(ev);
		return this;
	}

	/**
	 * Remove an event handler.
	 * 
	 * * <pre>
	 * 
	 * StorageRef storage = new StorageRef("your_app_key", "your_token");
	 * 
	 * TableRef tableRef = storage.table("your_table");
	 * 
	 * //Define handler function
	 * final OnItemSnapshot itemSnapshot = new OnItemSnapshot() {
     *       &#064;Override
     *       public void run(ItemSnapshot itemSnapshot) {
     *           if (itemSnapshot != null) {
     *               Log.d("TableRef", "Item : " + itemSnapshot.val());
     *           }
     *       }
     *   };
     *   
     *   // Add an update listener
     *   tableRef.on(StorageRef.StorageEvent.UPDATE, new ItemAttribute("your_primary_key_value"),itemSnapshot,new OnError() {
     *       &#064;Override
     *       public void run(Integer integer, String errorMessage) {
     *           Log.e("TableRef", "Error adding an update event listener: " + errorMessage);
     *       }
     *   });
     *
     *   Handler handler=new Handler();
	 *	
     *   final Runnable r = new Runnable()
     *   {
     *       public void run()
     *       {	 //remove the update listener
     *           tableRef.off(StorageRef.StorageEvent.UPDATE, new ItemAttribute("your_primary_key_value"),itemSnapshot);
     *       }
     *   };
	 *	 
	 *   // Stop listening after 5 seconds
     *   handler.postDelayed(r, 5000);
	 * 
	 * </pre>
	 * 
	 * @param eventType
	 * 		The type of the event to listen. Possible values: put, update, delete
	 * @param primary
	 * 		The primary key of the items to stop listen.
	 * @param onItemSnapshot
	 * 		The callback previously attached.
	 * @return Current table reference
	 */
	public TableRef off(StorageEvent eventType, ItemAttribute primary, OnItemSnapshot onItemSnapshot) {
		Event ev = new Event(eventType, this.name, primary, null, false, true, false, onItemSnapshot);
		context.removeEvent(ev);
		return this;
	}
	
	/**
	 * Retrieves the number of the table subscriptions and their respective connection metadata (limited to the first 100 subscriptions). Each subscriber is notified of changes made to the table.
	 * 
	 * @param onPresence
	 * 		Response from the server when the request was completed successfully.
	 * @param onError
	 * 		Response if client side validation failed or if an error was returned from the server.
	 * @return
	 */
	public TableRef presence(final OnPresence onPresence, final OnError onError) {
		context.presence(channel, onPresence, onError);
		return this;
	}
	
	/**
	 * Enables mobile push notifications for current table reference.
	 * 
	 * @return Current table reference
	 */
	public TableRef enablePushNotifications() {
	  pushNotificationsEnabled = true;
	  return this;
	}
	
   /**
   * Disables mobile push notifications for current table reference.
   * 
   * @return Current table reference
   */
  public TableRef disablePushNotifications() {
    pushNotificationsEnabled = false;
    context.disablePushNotificationsForChannels(this.name);
    return this;
  }
	
}
