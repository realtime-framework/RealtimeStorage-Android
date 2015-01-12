package co.realtime.storage;

import java.util.LinkedHashMap;


public class ItemSnapshot {
	TableRef tableRef;
	LinkedHashMap<String, ItemAttribute> value;
	ItemAttribute primaryValue;
	ItemAttribute secondaryValue;
	
	ItemSnapshot(TableRef tableRef, LinkedHashMap<String, ItemAttribute> value, ItemAttribute primaryValue, ItemAttribute secondaryValue){
		this.tableRef = tableRef;
		this.value = value;
		this.primaryValue = primaryValue;
		this.secondaryValue = secondaryValue;
	}
	
	/**
	 * Creates a new item reference object.
	 * 
	 * @return A reference to item
	 */
	public ItemRef ref(){
		return new ItemRef(this.tableRef.context, this.tableRef, this.primaryValue, this.secondaryValue);
	}
	
	/**
	 * Return the value of this snapshot.
	 * 
	 * @return The linked hash map containing the item attributes with properties names as a keys.
	 */
	public LinkedHashMap<String, ItemAttribute> val(){
		return this.value;		
	}

}
