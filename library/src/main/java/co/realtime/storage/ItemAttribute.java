package co.realtime.storage;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonValue;

/**
 * Class representing item's attribute. Can hold values of instance String and Number
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ItemAttribute implements Comparable<ItemAttribute>{
	Boolean isString;
	Object value;

	public ItemAttribute(String str){
		isString = true;
		value = str;
	}

	public ItemAttribute(Number num){
		isString = false;
		value = num;
	}		

	/**
	 * Returns the value of attribute
	 * @return the value
	 */
	@JsonValue
	@SuppressWarnings("unchecked")
	public <T> T get(){
		return (T) (this.isString ? (String)this.value : (Number)this.value);
	}

	/**
	 * Checks if attribute is string type
	 * @return true if is string type
	 */
	@JsonIgnore
	public Boolean isString(){
		return this.isString;
	}
	
	/**
	 * Checks if attribute is number type
	 * @return true if is number type
	 */
	@JsonIgnore
	public Boolean isNumber(){
		return !this.isString;
	}

	public String toString(){
		return (this.isString) ? (String)this.value : ((Number)this.value).toString();
	}

	//@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public int compareTo(ItemAttribute si) {
		if(this.isString)
			return ((String)this.value).compareTo(si.toString());
		if(si.isString())
			return this.value.toString().compareTo(si.toString());
		if (((Number)this.value).doubleValue() < ((Number)si.get()).doubleValue())
			return -1;				
		if (((Number)this.value).doubleValue() > ((Number)si.get()).doubleValue())
			return 1;				
		return 0;
		/*
		if(this.isString){
			return ((String)this.value).compareTo(si.toString());
		} else {
			if(si.isNumber()){
				if (((Number)this.value).doubleValue() < ((Number)si.get()).doubleValue())
					return -1;				
				if (((Number)this.value).doubleValue() > ((Number)si.get()).doubleValue())
					return 1;				
				return 0;
			} else { //Number vs String
				return this.value.toString().compareTo(si.toString());
			}
		}*/		
	}
	/*
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((value == null) ? 0 : value.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null){
			return false;
		}
		if (obj instanceof ItemAttribute){
			ItemAttribute other = (ItemAttribute) obj;
			if (value == null) {
				if (other.value != null) {
					return false;
				} else { 
					return true;
				}
			}
			if (this.compareTo(other)!=0) {
				return false;
			} else {
				return true;
			}
		}else{			
			return false;
		}
	}
	 */
}
