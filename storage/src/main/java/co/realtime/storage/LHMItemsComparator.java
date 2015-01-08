package co.realtime.storage;

import java.util.Comparator;
import java.util.LinkedHashMap;

class LHMItemsComparator implements Comparator<LinkedHashMap<String, ItemAttribute>> {
	String primaryKey;
	
	public LHMItemsComparator(String primaryKey){
		this.primaryKey = primaryKey;
	}
	
	@Override
	public int compare(LinkedHashMap<String, ItemAttribute> lhm1, LinkedHashMap<String, ItemAttribute> lhm2) {
		ItemAttribute si1 = lhm1.get(this.primaryKey);
		ItemAttribute si2 = lhm2.get(this.primaryKey);
		return si1.compareTo(si2);		
	}

}
