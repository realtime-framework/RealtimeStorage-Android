package co.realtime.storage;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;

import co.realtime.storage.Rest.RestType;
import co.realtime.storage.StorageRef.StorageEvent;
import co.realtime.storage.entities.TableMetadata;
import co.realtime.storage.ext.OnError;
import co.realtime.storage.ext.OnItemSnapshot;
import co.realtime.storage.ext.OnPresence;
import co.realtime.storage.ext.OnTableMetadata;

public class ItemRef {
	StorageContext context;
	TableRef table;
	ItemAttribute primaryKeyValue;
	ItemAttribute secondaryKeyValue;
	private Boolean pushNotificationsEnabled;	
	String channel;
	
	ItemRef(StorageContext context, TableRef table, ItemAttribute primaryKeyValue, ItemAttribute secondaryKeyValue){
		this.context = context;
		this.table = table;
		this.primaryKeyValue = primaryKeyValue;
		this.secondaryKeyValue = secondaryKeyValue;
		this.channel = "rtcs_" + table.name() + ":" + primaryKeyValue.get().toString();
		
		if(secondaryKeyValue != null) {
			channel += secondaryKeyValue.get().toString();
		}
		this.pushNotificationsEnabled = table.pushNotificationsEnabled;
	}
	
	private void _del(OnItemSnapshot onItemSnapshot, OnError onError){
		TableMetadata tm = context.getTableMeta(this.table.name);
		PostBodyBuilder pbb = new PostBodyBuilder(context);		
		pbb.addObject("table", this.table.name);
		LinkedHashMap<String, Object> key = new LinkedHashMap<String, Object>();
		key.put("primary", this.primaryKeyValue);
		if(tm.getSecondaryKeyName() != null)
			key.put("secondary", this.secondaryKeyValue);
		pbb.addObject("key", key);
		Rest r = new Rest(context, RestType.DELETEITEM, pbb, this.table);
		r.onError = onError;
		r.onItemSnapshot = onItemSnapshot;
		context.processRest(r);
		
	}
	
	/**
	 * Deletes an item specified by this reference
	 * 
	 * <pre>
	 * StorageRef storage = new StorageRef("your_app_key", "your_token");
	 * 
	 * TableRef tableRef = storage.table("your_table");
	 * 
	 * ItemRef itemRef = tableRef.item(new ItemAttribute("your_primary_key_value"), 
	 * 								new ItemAttribute("your_secondary_key_value"));
	 * 
	 * itemRef.del(new OnItemSnapshot() {
     *       &#064;Override
     *       public void run(ItemSnapshot itemSnapshot) {
     *           if(itemSnapshot != null){
     *               Log.d("ItemRef", "Item deleted: " + itemSnapshot.val());
     *           }
     *       }
     *   }, new OnError() {
     *       &#064;Override
     *       public void run(Integer integer, String errorMessage) {
     *           Log.e("ItemRef", "Error deleting item: " + errorMessage);
     *       }
     *   });
	 * 
	 * </pre>
	 * 
	 * @param onItemSnapshot
	 * 		The callback to call with the snapshot of affected item as an argument, when the operation is completed.
	 * @param onError
	 * 		The callback to call if an exception occurred
	 */
	public void del(final OnItemSnapshot onItemSnapshot, final OnError onError){
		TableMetadata tm = context.getTableMeta(this.table.name);
		if(tm == null){
			this.table.meta(new OnTableMetadata(){
				@Override
				public void run(TableMetadata tableMetadata) {
					_del(onItemSnapshot, onError);
				}				
			}, onError);
		} else {
			this._del(onItemSnapshot, onError);
		}
		return;
	}
	
	void _get(OnItemSnapshot onItemSnapshot, OnError onError, boolean endWithNull){
		TableMetadata tm = context.getTableMeta(this.table.name);
		PostBodyBuilder pbb = new PostBodyBuilder(context);	
		pbb.addObject("table", this.table.name);
		LinkedHashMap<String, Object> key = new LinkedHashMap<String, Object>();
		key.put("primary", this.primaryKeyValue);
		if(tm.getSecondaryKeyName() != null)
			key.put("secondary", this.secondaryKeyValue);
		pbb.addObject("key", key);
		Rest r = new Rest(context, RestType.GETITEM, pbb, this.table);
		r.onError = onError;
		r.onItemSnapshot = onItemSnapshot;
		r.endWithNull = endWithNull;
		context.processRest(r);
	
	}
	
	/**
	 * Gets an item snapshot specified by this item reference.
	 * 
	 * <pre>
	 * StorageRef storage = new StorageRef("your_app_key", "your_token");
	 * 
	 * TableRef tableRef = storage.table("your_table");
	 * 
	 * ItemRef itemRef = tableRef.item(new ItemAttribute("your_primary_key_value"), 
	 * 								new ItemAttribute("your_secondary_key_value"));
	 * 
	 * itemRef.get(new OnItemSnapshot() {
     *       &#064;Override
     *       public void run(ItemSnapshot itemSnapshot) {
     *           if(itemSnapshot != null){
     *               Log.d("ItemRef", "Item retrieved: " + itemSnapshot.val());
     *           }
     *       }
     *   },new OnError() {
     *       &#064;Override
     *       public void run(Integer integer, String errorMessage) {
     *           Log.e("ItemRef", "Error retrieving item: " + errorMessage);
     *       }
     *   });
	 * 
	 * </pre>
	 * 
	 * @param onItemSnapshot
	 * 		The callback to call with the snapshot of affected item as an argument, when the operation is completed.
	 * @param onError
	 * 		The callback to call if an exception occurred
	 * @return
	 * 		Current item reference
	 */
	public ItemRef get(final OnItemSnapshot onItemSnapshot, final OnError onError){
		TableMetadata tm = context.getTableMeta(this.table.name);
		if(tm == null){
			this.table.meta(new OnTableMetadata(){
				@Override
				public void run(TableMetadata tableMetadata) {
					_get(onItemSnapshot, onError, false);
				}				
			}, onError);
		} else {
			this._get(onItemSnapshot, onError, false);
		}
		return this;
	}
	
	
	void _set(LinkedHashMap<String, ItemAttribute> item, OnItemSnapshot onItemSnapshot, OnError onError){
		TableMetadata tm = context.getTableMeta(this.table.name);
		String primaryKeyName = null;
		String secondaryKeyName = null;
		primaryKeyName = tm.getPrimaryKeyName();
		secondaryKeyName = tm.getSecondaryKeyName();
		PostBodyBuilder pbb = new PostBodyBuilder(context);	
		pbb.addObject("table", this.table.name);
		LinkedHashMap<String, Object> key = new LinkedHashMap<String, Object>();
		key.put("primary", this.primaryKeyValue);
		if(tm.getSecondaryKeyName() != null)
			key.put("secondary", this.secondaryKeyValue);
		pbb.addObject("key", key);
		LinkedHashMap<String, ItemAttribute> itemToPut = new LinkedHashMap<String, ItemAttribute>();
		for(Map.Entry<String, ItemAttribute> entry : item.entrySet()){
			String eKey = entry.getKey();
			//ItemAttribute eValue = entry.getValue();
			if(!eKey.equals(primaryKeyName) && (!eKey.equals(secondaryKeyName))){
				itemToPut.put(eKey, entry.getValue());
			}
		}
		/*
		item.remove(primaryKeyName);
		if(secondaryKeyName != null)
			item.remove(secondaryKeyName);
		*/	
		pbb.addObject("item", itemToPut);
		Rest r = new Rest(context, RestType.UPDATEITEM, pbb, this.table);
		r.onError = onError;
		r.onItemSnapshot = onItemSnapshot;
		context.processRest(r);
	
	}	
	
	/**
	 * Updates the stored item specified by this item reference.
	 * 
	 * <pre>
	 * StorageRef storage = new StorageRef("your_app_key", "your_token");
	 * 
	 * TableRef tableRef = storage.table("your_table");
	 * 
	 * ItemRef itemRef = tableRef.item(new ItemAttribute("your_primary_key_value"), 
	 * 								new ItemAttribute("your_secondary_key_value"));
	 * 
	 * LinkedHashMap lhm = new LinkedHashMap();
     *   // Put elements to the map
     *   lhm.put("your_property","your_value");
     *   
     *   itemRef.set(lhm,new OnItemSnapshot() {
     *       &#064;Override
     *       public void run(ItemSnapshot itemSnapshot) {
     *           if(itemSnapshot != null){
     *               Log.d("ItemRef", "Item set: " + itemSnapshot.val());
     *           }
     *       }
     *   }, new OnError() {
     *       &#064;Override
     *       public void run(Integer integer, String errorMessage) {
     *           Log.e("ItemRef", "Error setting item: " + errorMessage);
     *       }
     *   });
	 * 
	 * </pre>
	 * 
	 * @param item
	 * 		The new properties of item to be updated.
	 * @param onItemSnapshot
	 * 		The callback to call with the snapshot of affected item as an argument, when the operation is completed.
	 * @param onError
	 * 		The callback to call if an exception occurred
	 * @return
	 * 		Current item reference
	 */
	public ItemRef set(final LinkedHashMap<String, ItemAttribute> item, final OnItemSnapshot onItemSnapshot, final OnError onError){
		TableMetadata tm = context.getTableMeta(this.table.name);
		if(tm == null){
			this.table.meta(new OnTableMetadata(){
				@Override
				public void run(TableMetadata tableMetadata) {
					_set(item, onItemSnapshot, onError);
				}				
			}, onError);
		} else {
			this._set(item, onItemSnapshot, onError);
		}
		return this;
	}
	
	/**
	 * Attach a listener to run the callback every time the event type occurs for this item.
	 * 
	 * <pre>
	 * StorageRef storage = new StorageRef("your_app_key", "your_token");
	 * 
	 * TableRef tableRef = storage.table("your_table");
	 * 
	 * ItemRef itemRef = tableRef.item(new ItemAttribute("your_primary_key_value"), 
	 * 								new ItemAttribute("your_secondary_key_value"));
	 * 
	 * itemRef.on(StorageRef.StorageEvent.UPDATE,new OnItemSnapshot() {
     *       &#064;Override
     *       public void run(ItemSnapshot itemSnapshot) {
     *           if(itemSnapshot != null){
     *               Log.d("ItemRef", "Item updated : " + itemSnapshot.val());
     *           }
     *       }
     *   }, new OnError() {
     *       &#064;Override
     *       public void run(Integer integer, String errorMessage) {
     *           Log.e("ItemRef", "Error updating item: " + errorMessage);
     *       }
     *   });
	 *
	 * </pre>
	 * 
	 * @param eventType
	 * 		The type of the event to listen. Possible values: put, update, delete.
	 * @param onItemSnapshot
	 * 		The callback to run when the event occurs. The function is called with the snapshot of affected item as argument. If the event type is "put", it will immediately trigger a "get" to retrieve the initial state and run the callback with the item snapshot as argument.  Note: If you are using GCM the value of received ItemSnapshot can be null.
	 * @param onError 
	 * 		Response if client side validation failed or if an error was returned from the server.
	 * @return
	 * 		Current item reference
	 */
	public ItemRef on(StorageEvent eventType, final OnItemSnapshot onItemSnapshot, final OnError onError) {
		if(eventType == StorageEvent.PUT) {
			this.get(onItemSnapshot, onError);
		}
		Event ev = new Event(eventType, this.table.name, this.primaryKeyValue, this.secondaryKeyValue, false, false, pushNotificationsEnabled,  onItemSnapshot);
		context.addEvent(ev);
		return this;
	}

	
	/**
	 * Attach a listener to run the callback every time the event type occurs for this item.
	 * 
	 * <pre>
	 * StorageRef storage = new StorageRef("your_app_key", "your_token");
	 * 
	 * TableRef tableRef = storage.table("your_table");
	 * 
	 * ItemRef itemRef = tableRef.item(new ItemAttribute("your_primary_key_value"), 
	 * 								new ItemAttribute("your_secondary_key_value"));
	 * 
	 * itemRef.on(StorageRef.StorageEvent.UPDATE,new OnItemSnapshot() {
     *       &#064;Override
     *       public void run(ItemSnapshot itemSnapshot) {
     *           if(itemSnapshot != null){
     *               Log.d("ItemRef", "Item updated : " + itemSnapshot.val());
     *           }
     *       }
     *   });
     *   
	 * </pre>
	 * 
	 * @param eventType
	 * 		The type of the event to listen. Possible values: put, update, delete.
	 * @param onItemSnapshot
	 * 		The callback to run when the event occurs. The function is called with the snapshot of affected item as argument. If the event type is "put", it will immediately trigger a "get" to retrieve the initial state and run the callback with the item snapshot as argument.  Note: If you are using GCM the value of received ItemSnapshot can be null.
	 * @return
	 * 		Current item reference
	 */
	public ItemRef on(StorageEvent eventType, final OnItemSnapshot onItemSnapshot) {
		return on(eventType, onItemSnapshot, null);
	}
	
	/**
	 *  Attach a listener to run the callback only one the event type occurs for this item.
	 *  
	 * <pre>
	 * StorageRef storage = new StorageRef("your_app_key", "your_token");
	 * 
	 * TableRef tableRef = storage.table("your_table");
	 * 
	 * ItemRef itemRef = tableRef.item(new ItemAttribute("your_primary_key_value"), 
	 * 								new ItemAttribute("your_secondary_key_value"));
	 * 
	 * itemRef.once(StorageRef.StorageEvent.UPDATE,new OnItemSnapshot() {
     *       &#064;Override
     *       public void run(ItemSnapshot itemSnapshot) {
     *           if(itemSnapshot != null){
     *               Log.d("ItemRef", "Item updated: " + itemSnapshot.val());
     *           }
     *       }
     *   }, new OnError() {
     *       &#064;Override
     *       public void run(Integer integer, String errorMessage) {
     *           Log.e("ItemRef", "Error adding an update event listener: " + errorMessage);
     *       }
     *   });
     *   
	 * </pre>
	 * 
	 * @param eventType
	 * 		The type of the event to listen. Possible values: put, update, delete.
	 * @param onItemSnapshot
	 * 		The callback to run when the event occurs. The function is called with the snapshot of affected item as argument.
	 * @param onError 
	 * 		Response if client side validation failed or if an error was returned from the server.
	 * @return
	 * 		Current item reference
	 */
	public ItemRef once(StorageEvent eventType, final OnItemSnapshot onItemSnapshot, final OnError onError) {
		if(eventType == StorageEvent.PUT) {
			this.get(onItemSnapshot, onError);
		}
		Event ev = new Event(eventType, this.table.name, this.primaryKeyValue, this.secondaryKeyValue, true, false, pushNotificationsEnabled, onItemSnapshot);
		context.addEvent(ev);
		return this;
	}

	
	/**
	 *  Attach a listener to run the callback only one the event type occurs for this item.
	 *  
	 * <pre>
	 * 
	 * StorageRef storage = new StorageRef("your_app_key", "your_token");
	 * 
	 * TableRef tableRef = storage.table("your_table");
	 * 
	 * ItemRef itemRef = tableRef.item(new ItemAttribute("your_primary_key_value"), 
	 * 								new ItemAttribute("your_secondary_key_value"));
	 * 
	 * itemRef.once(StorageRef.StorageEvent.UPDATE,new OnItemSnapshot() {
     *       &#064;Override
     *       public void run(ItemSnapshot itemSnapshot) {
     *           if(itemSnapshot != null){
     *               Log.d("ItemRef", "Item updated: " + itemSnapshot.val());
     *           }
     *       }
     *   });
	 * 
	 * </pre>
	 * 
	 * @param eventType
	 * 		The type of the event to listen. Possible values: put, update, delete.
	 * @param onItemSnapshot
	 * 		The callback to run when the event occurs. The function is called with the snapshot of affected item as argument.
	 * @return
	 * 		Current item reference
	 */
	public ItemRef once(StorageEvent eventType, final OnItemSnapshot onItemSnapshot) {
		return once(eventType, onItemSnapshot, null);
	}
	
	/**
	 * Remove an event listener
	 * 
	 * <pre>
	 * StorageRef storage = new StorageRef("your_app_key", "your_token");
	 * 
	 * TableRef tableRef = storage.table("your_table");
	 * 
	 * ItemRef itemRef = tableRef.item(new ItemAttribute("your_primary_key_value"), 
	 * 								new ItemAttribute("your_secondary_key_value"));
	 * 
	 * final OnItemSnapshot itemSnapshot = new OnItemSnapshot() {
     *       &#064;Override
     *       public void run(ItemSnapshot itemSnapshot) {
     *           if (itemSnapshot != null) {
     *               Log.d("ItemRef", "Item : " + itemSnapshot.val());
     *           }
     *       }
     *   };
     *
     *   itemRef.on(StorageRef.StorageEvent.UPDATE,itemSnapshot,new OnError() {
     *       &#064;Override
     *       public void run(Integer integer, String errorMessage) {
     *           Log.e("ItemRef", "Error adding an update event listener: " + errorMessage);
     *       }
     *   });
     *
     *   Handler handler=new Handler();
	 *
     *   final Runnable r = new Runnable()
     *   {
     *       public void run()
     *       {
     *           itemRef.off(StorageRef.StorageEvent.UPDATE, itemSnapshot);
     *       }
     *   };
	 *
     *   handler.postDelayed(r, 5000);
     *   
	 * </pre>
	 * 
	 * @param eventType
	 * 		The type of the event to listen. Possible values: put, update, delete.
	 * @param onItemSnapshot
	 * 		The callback previously attached.
	 * @return
	 * 		Current item reference
	 */
	public ItemRef off(StorageEvent eventType, OnItemSnapshot onItemSnapshot){
		Event ev = new Event(eventType, this.table.name, this.primaryKeyValue, this.secondaryKeyValue, false, false, false, onItemSnapshot);
		context.removeEvent(ev);
		return this;
	}
	
	void _in_de_cr(String property, Number value, boolean isIncr, OnItemSnapshot onItemSnapshot, OnError onError){
		TableMetadata tm = context.getTableMeta(this.table.name);
		PostBodyBuilder pbb = new PostBodyBuilder(context);	
		pbb.addObject("table", this.table.name);
		LinkedHashMap<String, Object> key = new LinkedHashMap<String, Object>();
		key.put("primary", this.primaryKeyValue);
		if(tm.getSecondaryKeyName() != null)
			key.put("secondary", this.secondaryKeyValue);
		pbb.addObject("key", key);
		pbb.addObject("property", property);
		if(value != null){
			pbb.addObject("value", value);
		}
		Rest r = new Rest(context, isIncr ? RestType.INCR : RestType.DECR, pbb, this.table);
		r.onError = onError;
		r.onItemSnapshot = onItemSnapshot;
		context.processRest(r);
	}
	
	/**
	 * Increments a given attribute of an item. If the attribute doesn't exist, it is set to zero before the operation.
	 * 
	 * <pre>
	 * StorageRef storage = new StorageRef("your_app_key", "your_token");
	 * 
	 * TableRef tableRef = storage.table("your_table");
	 * 
	 * ItemRef itemRef = tableRef.item(new ItemAttribute("your_primary_key_value"), 
	 * 								new ItemAttribute("your_secondary_key_value"));
	 * 
	 * itemRef.incr("your_property",10, new OnItemSnapshot() {
     *       &#064;Override
     *       public void run(ItemSnapshot itemSnapshot) {
     *           if(itemSnapshot != null){
     *               Log.d("ItemRef", "Item incremented: " + itemSnapshot.val());
     *           }
     *       }
     *   }, new OnError() {
     *       &#064;Override
     *       public void run(Integer integer, String errorMessage) {
     *           Log.e("ItemRef", "Error incrementing item: " + errorMessage);
     *       }
     *   });
	 * 
	 * </pre>
	 * 
	 * @param property The name of the item attribute.
	 * @param value The number to add
	 * @param onItemSnapshot The callback invoked once the attribute has been incremented successfully. The callback is called with the snapshot of the item as argument.
	 * @param onError The callback invoked if an error occurred. Called with the error description.
	 * @return Current item reference
	 */
	public ItemRef incr(final String property, final Number value, final OnItemSnapshot onItemSnapshot, final OnError onError){
		TableMetadata tm = context.getTableMeta(this.table.name);
		if(tm == null){
			this.table.meta(new OnTableMetadata(){
				@Override
				public void run(TableMetadata tableMetadata) {
					_in_de_cr(property, value, true, onItemSnapshot, onError);
				}				
			}, onError);
		} else {
			this._in_de_cr(property, value, true, onItemSnapshot, onError);
		}
		return this;		
	}
	
	/**
	 * Increments by one a given attribute of an item. If the attribute doesn't exist, it is set to zero before the operation.
	 * 
	 * <pre>
	 * StorageRef storage = new StorageRef("your_app_key", "your_token");
	 * 
	 * TableRef tableRef = storage.table("your_table");
	 * 
	 * ItemRef itemRef = tableRef.item(new ItemAttribute("your_primary_key_value"), 
	 * 								new ItemAttribute("your_secondary_key_value"));
	 * 
	 * itemRef.incr("your_property",new OnItemSnapshot() {
     *       &#064;Override
     *       public void run(ItemSnapshot itemSnapshot) {
     *           if(itemSnapshot != null){
     *               Log.d("ItemRef", "Item incremented: " + itemSnapshot.val());
     *           }
     *       }
     *   }, new OnError() {
     *       &#064;Override
     *       public void run(Integer integer, String errorMessage) {
     *           Log.e("ItemRef", "Error incrementing item: " + errorMessage);
     *       }
     *   });
	 * 
	 * </pre>
	 * 
	 * @param property The name of the item attribute.
	 * @param onItemSnapshot The callback invoked once the attribute has been incremented successfully. The callback is called with the snapshot of the item as argument.
	 * @param onError The callback invoked if an error occurred. Called with the error description.
	 * @return Current item reference
	 */
	public ItemRef incr(final String property, final OnItemSnapshot onItemSnapshot, final OnError onError){
		return this.incr(property, null, onItemSnapshot, onError);
	}
	
	/**
	 * Decrements the value of an items attribute. If the attribute doesn't exist, it is set to zero before the operation.
	 * 
	 * <pre>
	 * 
	 * StorageRef storage = new StorageRef("your_app_key", "your_token");
	 * 
	 * TableRef tableRef = storage.table("your_table");
	 * 
	 * ItemRef itemRef = tableRef.item(new ItemAttribute("your_primary_key_value"), 
	 * 								new ItemAttribute("your_secondary_key_value"));
	 * 
	 * itemRef.decr("your_property",10 , new OnItemSnapshot() {
     *       &#064;Override
     *       public void run(ItemSnapshot itemSnapshot) {
     *           if(itemSnapshot != null){
     *               Log.d("ItemRef", "Item decremented: " + itemSnapshot.val());
     *           }
     *       }
     *   }, new OnError() {
     *       &#064;Override
     *       public void run(Integer integer, String errorMessage) {
     *           Log.e("ItemRef", "Error decrementing: " + errorMessage);
     *       }
     *   });
	 * 
	 * </pre>
	 * 
	 * @param property The name of the item attribute.
	 * @param value The number to subtract
	 * @param onItemSnapshot The callback invoked once the attribute has been decremented successfully. The callback is called with the snapshot of the item as argument.
	 * @param onError The callback invoked if an error occurred. Called with the error description.
	 * @return Current item reference
	 */
	public ItemRef decr(final String property, final Number value, final OnItemSnapshot onItemSnapshot, final OnError onError){
		TableMetadata tm = context.getTableMeta(this.table.name);
		if(tm == null){
			this.table.meta(new OnTableMetadata(){
				@Override
				public void run(TableMetadata tableMetadata) {
					_in_de_cr(property, value, false, onItemSnapshot, onError);
				}				
			}, onError);
		} else {
			this._in_de_cr(property, value, false, onItemSnapshot, onError);
		}
		return this;		
	}
	
	/**
	 * Decrements the value by one of an items attribute. If the attribute doesn't exist, it is set to zero before the operation.
	 * 
	 * <pre>
	 * 
	 * StorageRef storage = new StorageRef("your_app_key", "your_token");
	 * 
	 * TableRef tableRef = storage.table("your_table");
	 * 
	 * ItemRef itemRef = tableRef.item(new ItemAttribute("your_primary_key_value"), 
	 * 								new ItemAttribute("your_secondary_key_value"));
	 * 
	 * itemRef.decr("your_property",new OnItemSnapshot() {
     *       &#064;Override
     *       public void run(ItemSnapshot itemSnapshot) {
     *           if(itemSnapshot != null){
     *               Log.d("ItemRef", "Item decremented: " + itemSnapshot.val());
     *           }
     *       }
     *   }, new OnError() {
     *       &#064;Override
     *       public void run(Integer integer, String errorMessage) {
     *           Log.e("ItemRef", "Error decrementing: " + errorMessage);
     *       }
     *   });
	 * </pre>
	 * 
	 * @param property The name of the item attribute.
	 * @param onItemSnapshot The callback invoked once the attribute has been decremented successfully. The callback is called with the snapshot of the item as argument.
	 * @param onError The callback invoked if an error occurred. Called with the error description.
	 * @return Current item reference
	 */
	public ItemRef decr(final String property, final OnItemSnapshot onItemSnapshot, final OnError onError){
		return this.decr(property, null, onItemSnapshot, onError);
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
	public ItemRef presence(final OnPresence onPresence, final OnError onError) {
		context.presence(channel, onPresence, onError);
		return this;
	}
	
   /**
   * Enables mobile push notifications for current item reference.
   * 
   * @return Current item reference
   */
  public ItemRef enablePushNotifications(){
    pushNotificationsEnabled = true;
    return this;
  }
  
   /**
   * Disables mobile push notifications for current item reference.
   * 
   * @return Current item reference
   */
  public ItemRef disablePushNotifications(){
    pushNotificationsEnabled = false;    
    Event ev = new Event(null, this.table.name, this.primaryKeyValue, this.secondaryKeyValue, false, false, pushNotificationsEnabled,  null);
    ArrayList<String> channels = new ArrayList<String>();
    channels.add(ev.getChannelName());
    context.disablePushNotificationsForChannels(channels);
    
    return this;
  }
}
