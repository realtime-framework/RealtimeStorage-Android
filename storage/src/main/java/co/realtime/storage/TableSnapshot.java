package co.realtime.storage;

public class TableSnapshot {
	StorageContext context;
	String name;
	
	TableSnapshot(StorageContext context, String name){
		this.context = context;
		this.name = name;
	}
	
	/**
	 * Retrieves the name of the table
	 * 
	 * @return Table name
	 */
	public String val(){
		return this.name;
	}
	
	/**
	 * Returns a table reference
	 * 
	 * @return The table reference
	 */
	public TableRef ref(){
		return new TableRef(this.context, this.name);
	}
}
