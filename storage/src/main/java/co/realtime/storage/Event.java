package co.realtime.storage;

import co.realtime.storage.StorageRef.StorageEvent;
import co.realtime.storage.ext.OnItemSnapshot;

class Event {
	StorageEvent type;
	String tableName;
	ItemAttribute primary;
	ItemAttribute secondary;
	Boolean isOnce;
	OnItemSnapshot onItemSnapshot;
	Boolean pushNotificationsEnabled;
	Boolean isOnTable;
	/*
	Event(StorageEvent type, String tableName, ItemAttribute primary, ItemAttribute secondary, Boolean isOnce, Boolean isOnTable, OnItemSnapshot onItemSnapshot){
		this.type = type;
		this.tableName = tableName;
		this.primary = primary;
		this.secondary = secondary;
		this.isOnce = isOnce;
		this.onItemSnapshot = onItemSnapshot;		
		this.isOnTable = isOnTable;
		this.pushNotificationsEnabled = false; 
	}*/

	Event(StorageEvent type, String tableName, ItemAttribute primary, ItemAttribute secondary, Boolean isOnce, Boolean isOnTable, Boolean pushNotificationsEnabled, OnItemSnapshot onItemSnapshot){
		this.type = type;
		this.tableName = tableName;
		this.primary = primary;
		this.secondary = secondary;
		this.isOnce = isOnce;
		this.onItemSnapshot = onItemSnapshot;
		this.isOnTable = isOnTable;
		this.pushNotificationsEnabled = pushNotificationsEnabled;
	}
	
	public void fire(ItemSnapshot item){
		if(this.onItemSnapshot != null)
			this.onItemSnapshot.run(item);
	}
	
	public String getChannelName(){
		if(this.primary==null && this.secondary==null)
			return String.format("rtcs_%s", this.tableName);
		if(this.secondary==null)
			return String.format("rtcs_%s:%s", this.tableName, this.primary.toString());
		return String.format("rtcs_%s:%s_%s", this.tableName, this.primary.toString(), this.secondary.toString());
	}
}
