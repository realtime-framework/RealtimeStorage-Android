package co.realtime.storage;

//import com.fasterxml.jackson.databind.ser.impl.JsonSerializerMap;
import com.google.gson.Gson;

import org.json.JSONObject;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import co.realtime.storage.StorageRef.StorageDataType;
import co.realtime.storage.StorageRef.StorageOrder;
import co.realtime.storage.StorageRef.StorageProvisionLoad;
import co.realtime.storage.StorageRef.StorageProvisionType;
import co.realtime.storage.entities.TableMetadata;
import co.realtime.storage.ext.OnBooleanResponse;
import co.realtime.storage.ext.OnItemSnapshot;
import co.realtime.storage.ext.OnTableCreation;
import co.realtime.storage.ext.OnTableMetadata;
import co.realtime.storage.ext.OnTableUpdate;

//import co.realtime.storage.ext.OnItemSnapshot;
//import co.realtime.storage.ext.OnTableCreation;
//import co.realtime.storage.ext.OnTableMetadata;
import co.realtime.storage.ext.OnTableSnapshot;


class ProcessRestResponse {
	public static void processIsAuthenticated(Map<String, Object> response, OnBooleanResponse onBooleanResponse){
		if(onBooleanResponse == null) return;
		onBooleanResponse.run((Boolean)response.get("data"));
	}
	
	@SuppressWarnings("unchecked")
	public static void processListTables(Map<String, Object> response, StorageContext context,  OnTableSnapshot onTableSnapshot){		
		if(onTableSnapshot == null) return;
		LinkedHashMap<String, Object> linkedHashMap = (LinkedHashMap<String, Object>)response.get("data");
		ArrayList<String> tables = (ArrayList<String>) linkedHashMap.get("tables");
		for(String tabName : tables){
			onTableSnapshot.run(new TableSnapshot(context, tabName));
		}
		onTableSnapshot.run(null);
	}
	
	@SuppressWarnings("unchecked")
	public static void processCreateTable(Map<String, Object> response, OnTableCreation onTableCreation){
		if(onTableCreation == null) return;
		LinkedHashMap<String, Object> linkedHashMap = (LinkedHashMap<String, Object>)response.get("data");
		String tableName = (String) linkedHashMap.get("table");
		Double creationDate = (Double) linkedHashMap.get("creationDate");
		String status = (String) linkedHashMap.get("status");
		onTableCreation.run(tableName, creationDate, status);
	}
	
	@SuppressWarnings("unchecked")
	public static void processDeleteTable(Map<String, Object> response, OnBooleanResponse onBooleanResponse){
		if(onBooleanResponse == null) return;
		LinkedHashMap<String, Object> linkedHashMap = (LinkedHashMap<String, Object>)response.get("data");
		String status = (String)linkedHashMap.get("status");
		if(status.equals("deleting")){
			onBooleanResponse.run(true);
		} else {
			onBooleanResponse.run(false);
		}
	}
	
	@SuppressWarnings("unchecked")
	public static void processDescribeTable(Map<String, Object> response, StorageContext context, OnTableMetadata onTableMetadata){		
		LinkedHashMap<String, Object> linkedHashMap = (LinkedHashMap<String, Object>)response.get("data");
		TableMetadata tm = new TableMetadata();
		tm.setApplicationKey((String) linkedHashMap.get("applicationKey"));
		tm.setName((String) linkedHashMap.get("name"));
		LinkedHashMap<String, Object> provisionType = (LinkedHashMap<String, Object>)linkedHashMap.get("provisionType");
		tm.setProvisionType( StorageProvisionType.values()[(Integer) provisionType.get("id") - 1]);
		LinkedHashMap<String, Object> provisionLoad = (LinkedHashMap<String, Object>)linkedHashMap.get("provisionLoad");
		tm.setProvisionLoad(StorageProvisionLoad.values()[(Integer) provisionLoad.get("id") - 1]);
		LinkedHashMap<String, Object> throughput = (LinkedHashMap<String, Object>)linkedHashMap.get("throughput");
		tm.setThroughputRead((Integer) throughput.get("read"));
		tm.setThroughputWrite((Integer) throughput.get("write"));
		tm.setCreationDate((Long) linkedHashMap.get("creationDate"));
		tm.setUpdateDate((Long) linkedHashMap.get("updateDate"));
		tm.setIsActive((Boolean) linkedHashMap.get("isActive"));
		LinkedHashMap<String, Object> key = (LinkedHashMap<String, Object>)linkedHashMap.get("key");
		LinkedHashMap<String, Object> primary = (LinkedHashMap<String, Object>)key.get("primary");
		tm.setPrimaryKeyName((String) primary.get("name"));
		tm.setPrimaryKeyType(StorageDataType.fromString((String)primary.get("dataType")));				
		LinkedHashMap<String, Object> secondary = (LinkedHashMap<String, Object>)key.get("secondary");
		if(secondary!=null){
			tm.setSecondaryKeyName((String) secondary.get("name"));
			tm.setSecondaryKeyType(StorageDataType.fromString((String) secondary.get("dataType")));
		}
		tm.setStatus((String) linkedHashMap.get("status"));
		tm.setSize(Long.parseLong(String.valueOf(linkedHashMap.get("size"))));
		tm.setItemCount(Long.parseLong(String.valueOf(linkedHashMap.get("itemCount"))));
		
		context.addTableMeta(tm);
		
		if(onTableMetadata != null)
			onTableMetadata.run(tm);
		
	}
	
	private static void _fireItemSnapshotCallback(OnItemSnapshot onItemSnapshot, TableRef tableRef, LinkedHashMap<String, ItemAttribute> item){
		TableMetadata tm = tableRef.context.getTableMeta(tableRef.name);
		String primary = tm.getPrimaryKeyName();
		String secondary = tm.getSecondaryKeyName();
		ItemAttribute primaryValue = item.get(primary);
		ItemAttribute secondaryValue = null;
		if(secondary!=null)
			secondaryValue = item.get(secondary);
		onItemSnapshot.run(new ItemSnapshot(tableRef, item, primaryValue, secondaryValue));
	}
	
	private static void fireItemSnapshotCallback(final OnItemSnapshot onItemSnapshot, final TableRef tableRef, final LinkedHashMap<String, ItemAttribute> item){
		TableMetadata tm = tableRef.context.getTableMeta(tableRef.name);
		if(tm==null){
			tableRef.meta(new OnTableMetadata(){
				@Override
				public void run(TableMetadata tableMetadata) {
					_fireItemSnapshotCallback(onItemSnapshot, tableRef, item);
				}}, null);
		} else {
			_fireItemSnapshotCallback(onItemSnapshot, tableRef, item);
		}
	}
	
	@SuppressWarnings("unchecked")
	public static void processPutItem(Map<String, Object> response, TableRef tableRef, OnItemSnapshot onItemSnapshot){
		if(onItemSnapshot==null) return;
		LinkedHashMap<String, Object> linkedHashMap = (LinkedHashMap<String, Object>)response.get("data");
		LinkedHashMap<String, ItemAttribute> item = convertItemMap(linkedHashMap);
		fireItemSnapshotCallback(onItemSnapshot, tableRef, item);
		//ItemSnapshot item = new ItemSnapshot(tableRef, itemMap);		
		//onItemSnapshot.run(item);
	}	
	
	@SuppressWarnings("unchecked")
	public static void processUpdateTable(Map<String, Object> response, OnTableUpdate onTableUpdate){
		if(onTableUpdate==null) return;
		LinkedHashMap<String, Object> linkedHashMap = (LinkedHashMap<String, Object>)response.get("data");
		String tableName = (String) linkedHashMap.get("table");
		String status = (String) linkedHashMap.get("status");
		onTableUpdate.run(tableName, status);
	}

	public static void processListItems(ArrayList<LinkedHashMap<String, Object>> allItems, TableRef tableRef, OnItemSnapshot onItemSnapshot, StorageOrder order, String sortKey, Long limit){
		if(onItemSnapshot==null) return;
		ArrayList<LinkedHashMap<String, ItemAttribute>> items = new ArrayList<LinkedHashMap<String, ItemAttribute>>();
		for(LinkedHashMap<String, Object> item : allItems){
			items.add(convertItemMap(item));
			//onItemSnapshot.run(new ItemSnapshot(tableRef, convertItemMap(item)));
		}
		if(order == StorageOrder.ASC){
			Collections.sort(items, new LHMItemsComparator(sortKey));
		}
		if(order == StorageOrder.DESC){
			Collections.sort(items, Collections.reverseOrder(new LHMItemsComparator(sortKey)));
		}
		/*
		for(LinkedHashMap<String, StorageItem> item : items){			
			onItemSnapshot.run(new ItemSnapshot(tableRef, item));
		}*/
		Long itemsToDeliver = (limit == null) ? items.size() : (limit > items.size() ? items.size() : limit );
		for(int l = 0; l < itemsToDeliver; l++){
			//onItemSnapshot.run(new ItemSnapshot(tableRef, items.get(l)));
			fireItemSnapshotCallback(onItemSnapshot, tableRef, items.get(l));
		}
		onItemSnapshot.run(null);
	}
	
	public static void processQueryItems(ArrayList<LinkedHashMap<String, Object>> allItems, TableRef tableRef, OnItemSnapshot onItemSnapshot){
		if(onItemSnapshot==null) return;
		for(LinkedHashMap<String, Object> item : allItems){		
			//onItemSnapshot.run(new ItemSnapshot(tableRef, convertItemMap(item)));
			LinkedHashMap<String, ItemAttribute> itemMap = convertItemMap(item);
			fireItemSnapshotCallback(onItemSnapshot, tableRef, itemMap);
		}
		onItemSnapshot.run(null);
	}	
	
	@SuppressWarnings("unchecked")
	public static void processDelItem(Map<String, Object> response, TableRef tableRef, OnItemSnapshot onItemSnapshot){
		if(onItemSnapshot==null) return;
		LinkedHashMap<String, Object> linkedHashMap = (LinkedHashMap<String, Object>)response.get("data");
		if(linkedHashMap == null || linkedHashMap.size()==0){
			onItemSnapshot.run(null);
			return;
		}
		//ItemSnapshot item = new ItemSnapshot(tableRef, convertItemMap(linkedHashMap));		
		//onItemSnapshot.run(item);
		LinkedHashMap<String, ItemAttribute> item = convertItemMap(linkedHashMap);
		fireItemSnapshotCallback(onItemSnapshot, tableRef, item);
	}	
	
	@SuppressWarnings("unchecked")
	public static void processGetItem(Map<String, Object> response, TableRef tableRef, OnItemSnapshot onItemSnapshot, boolean endWithNull){
		if(onItemSnapshot==null) return;
		LinkedHashMap<String, Object> linkedHashMap = (LinkedHashMap<String, Object>)response.get("data");
		if(linkedHashMap == null || linkedHashMap.size()==0){
			onItemSnapshot.run(null);
			return;
		}
		//ItemSnapshot item = new ItemSnapshot(tableRef, convertItemMap(linkedHashMap));		
		//onItemSnapshot.run(item);
		LinkedHashMap<String, ItemAttribute> item = convertItemMap(linkedHashMap);
		fireItemSnapshotCallback(onItemSnapshot, tableRef, item);
		if(endWithNull){
			onItemSnapshot.run(null);
		}
	}		
	
	@SuppressWarnings("unchecked")
	public static void processUpdateItem(Map<String, Object> response, TableRef tableRef, OnItemSnapshot onItemSnapshot){
		if(onItemSnapshot==null) return;
		LinkedHashMap<String, Object> linkedHashMap = (LinkedHashMap<String, Object>)response.get("data");
		if(linkedHashMap == null || linkedHashMap.size()==0){
			onItemSnapshot.run(null);
			return;
		}
		//ItemSnapshot item = new ItemSnapshot(tableRef, convertItemMap(linkedHashMap));		
		//onItemSnapshot.run(item);
		LinkedHashMap<String, ItemAttribute> item = convertItemMap(linkedHashMap);
		fireItemSnapshotCallback(onItemSnapshot, tableRef, item);
	}		
	
	public static LinkedHashMap<String, ItemAttribute> convertItemMap(LinkedHashMap<String, Object> map){
		LinkedHashMap<String, ItemAttribute> ret = new LinkedHashMap<String, ItemAttribute>();
		for (Entry<String, Object> entry : map.entrySet()){
			Object value = entry.getValue();
            if(value instanceof Number)
                ret.put(entry.getKey(), new ItemAttribute((Number)value));
            if(value instanceof String){
                ret.put(entry.getKey(), new ItemAttribute((String)value));
            }
            if(value instanceof LinkedHashMap){
                ret.put(entry.getKey(), new ItemAttribute(new Gson().toJson(value)));
            }
		}
		return ret;
	}
	
	@SuppressWarnings("unchecked")
	public static void processInDeCrResponse(Map<String, Object> response, TableRef tableRef, OnItemSnapshot onItemSnapshot){
		if(onItemSnapshot==null) return;
		LinkedHashMap<String, Object> linkedHashMap = (LinkedHashMap<String, Object>)response.get("data");
		LinkedHashMap<String, ItemAttribute> item = convertItemMap(linkedHashMap);
		fireItemSnapshotCallback(onItemSnapshot, tableRef, item);
	}
}
