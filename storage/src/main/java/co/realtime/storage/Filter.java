package co.realtime.storage;

import java.util.ArrayList;
import java.util.LinkedHashMap;

class Filter {
	StorageFilter operator;
	String itemName;
	ItemAttribute value;
	ItemAttribute valueEx;
	
	enum StorageFilter {
		EQUALS("equals"), NOTEQUAL("notEqual"), GREATEREQUAL("greaterEqual"), GREATERTHAN("greaterThan"), LESSEREQUAL("lessEqual"), LESSERTHAN("lessThan"),
		NOTNULL("notNull"), NULL("null"), CONTAINS("contains"), NOTCONTAINS("notContains"), BEGINSWITH("beginsWith"), BETWEEN("between");
		private final String strFilter;		
		private StorageFilter(String s){
			strFilter = s;
		}		
		public String toString() {
			return strFilter;
		}
	}
		
	Filter(StorageFilter operator, String itemName, ItemAttribute value, ItemAttribute valueEx){
		this.operator = operator;
		this.itemName = itemName;
		this.value = value;
		this.valueEx = valueEx;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((itemName == null) ? 0 : itemName.hashCode());
		result = prime * result + ((operator == null) ? 0 : operator.hashCode());		
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Filter other = (Filter) obj;
		if (itemName == null) {
			if (other.itemName != null)
				return false;
		} else if (!itemName.equals(other.itemName))
			return false;
		if (operator != other.operator)
			return false;
		return true;
	}
	
	public LinkedHashMap<String, Object> prepareForJSON(){
		LinkedHashMap<String, Object> lhm = new LinkedHashMap<String, Object>();
		lhm.put("operator", this.operator.toString());
		lhm.put("item", this.itemName);
		if(this.operator==StorageFilter.BETWEEN){
			ArrayList<Object> ar = new ArrayList<Object>();
			ar.add(value);
			ar.add(valueEx);
			lhm.put("value", ar);
		} else if(this.operator== StorageFilter.NULL || this.operator == StorageFilter.NOTNULL){
			//Don't put value because of the exception: Invalid number of argument(s) for the NULL ComparisonOperator.
		} else {
			lhm.put("value", this.value);
		}
		return lhm;
	}
}
