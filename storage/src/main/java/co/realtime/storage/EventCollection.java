package co.realtime.storage;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import co.realtime.storage.StorageRef.StorageEvent;

class EventCollection {
	LinkedHashMap<String, Set<Event>> puts;
	LinkedHashMap<String, Set<Event>> dels;
	LinkedHashMap<String, Set<Event>> upds;
	LinkedHashMap<String, Integer> evOnChannel;
	
	EventCollection(){
		puts = new LinkedHashMap<String, Set<Event>>();
		dels = new LinkedHashMap<String, Set<Event>>();
		upds = new LinkedHashMap<String, Set<Event>>();
		evOnChannel = new LinkedHashMap<String, Integer>(); 
	}
	
	public Boolean add(Event ev){
		//String channelName = ev.tableName;
		String channelName = ev.getChannelName();
		Boolean ret = (getNumberOfEvents(channelName)==0) ? true : false;
		LinkedHashMap<String, Set<Event>> list = getMap(ev.type);
		Set<Event> events = list.get(channelName);
		if(events == null){
			Set<Event> newSet = new HashSet<Event>();
			newSet.add(ev);
			list.put(channelName, newSet);
		} else {
			events.add(ev);
		}
		incNumberOfEvents(channelName);
		return ret;
	}
	
	LinkedHashMap<String, Set<Event>> getMap(StorageEvent type){
		switch(type){
		case DELETE: return dels;			
		case PUT: return puts;			
		case UPDATE: return upds;					
		}
		return null;
	}
	
	int getNumberOfEvents(String channelName){
		Integer i = evOnChannel.get(channelName);
		if(i == null)
			return 0;
		return i.intValue();	
	}
	
	void incNumberOfEvents(String channelName){
		int i = getNumberOfEvents(channelName);
		this.evOnChannel.put(channelName, new Integer(i+1));
	}
	
	void decNumberOfEvents(String channelName){
		int i = getNumberOfEvents(channelName);
		this.evOnChannel.put(channelName, new Integer(i-1));
	}

	public Boolean fireEvents(String tableName, StorageEvent eventType, ItemAttribute primary, ItemAttribute secondary, ItemSnapshot itemSnapshot) {
		LinkedHashMap<String, Set<Event>> list = getMap(eventType);
		Set<Event> events = list.get(tableName);
		Set<Event> evToRemove = new HashSet<Event>();
		if(events != null){
			for(Event ev : events){
				if(ev.primary == null && ev.secondary == null){ //event on all table
					ev.fire(itemSnapshot);
					if(ev.isOnce)
						evToRemove.add(ev);
				} else if(ev.primary.compareTo(primary) == 0 && ev.secondary == null) { //event on primary key
					ev.fire(itemSnapshot);
					if(ev.isOnce)
						evToRemove.add(ev);
				} else if(secondary != null) { //table has secondary key
					if(ev.primary.compareTo(primary) == 0 && ev.secondary.compareTo(secondary) == 0) {
						ev.fire(itemSnapshot);
						if(ev.isOnce)
							evToRemove.add(ev);
					}
				}					
			}
			for(Event ev : evToRemove){
				events.remove(ev);
				decNumberOfEvents(tableName);
			}
		}
		return (getNumberOfEvents(tableName)==0) ? true : false;
	}

	public Boolean fireEvents(String channelName, StorageEvent eventType, ItemSnapshot itemSnapshot) {
		LinkedHashMap<String, Set<Event>> list = getMap(eventType);
		Set<Event> events = list.get(channelName);
		Set<Event> evToRemove = new HashSet<Event>();
		if(events != null){
			for(Event ev : events){
				ev.fire(itemSnapshot);
				if(ev.isOnce)
					evToRemove.add(ev);					
			}
			for(Event ev : evToRemove){
				events.remove(ev);
				decNumberOfEvents(channelName);
			}
		}
		return (getNumberOfEvents(channelName)==0) ? true : false;		
	}
	
	public Boolean remove(Event evToRemove) {
		//String channelName = evToRemove.tableName;
		String channelName = evToRemove.getChannelName();
		LinkedHashMap<String, Set<Event>> list = getMap(evToRemove.type);
		Set<Event> events = list.get(channelName);
		Set<Event> toRemove = new HashSet<Event>();
		if(events != null){
			for(Event ev : events){
				if(ev.onItemSnapshot.equals(evToRemove.onItemSnapshot)){
					if(ev.secondary == null){
						if(evToRemove.secondary != null)
							continue;
					} else {
						if(ev.secondary.compareTo(evToRemove.secondary)!=0){
							continue;
						}
					}
					if(ev.primary == null){
						if(evToRemove.primary != null)
							continue;
					} else {
						if(ev.primary.compareTo(evToRemove.primary)!=0){
							continue;
						}
					}
					toRemove.add(ev);
				}
			}
			for(Event ev : toRemove){
				events.remove(ev);
				decNumberOfEvents(channelName);
			}		
		}
		return (getNumberOfEvents(channelName)==0) ? true : false;
	}
	
	public ArrayList<String> getChannelNames(String tableName, Boolean isOnTableRef){
		ArrayList<String> ret = new ArrayList<String>();
		ret.addAll(getChannelNamesFromEventMap(puts, tableName, isOnTableRef, ret));
		ret.addAll(getChannelNamesFromEventMap(dels, tableName, isOnTableRef, ret));
		ret.addAll(getChannelNamesFromEventMap(upds, tableName, isOnTableRef, ret));
		return ret;		
	}
	
	private ArrayList<String> getChannelNamesFromEventMap(LinkedHashMap<String, Set<Event>> eventMap, String tableName, Boolean isOnTableRef, ArrayList<String> alreadyCollected ){
		ArrayList<String> ret = new ArrayList<String>();
		for(Map.Entry<String, Set<Event>> entry : eventMap.entrySet()){
			String channelName = entry.getKey();
			if(alreadyCollected.contains(channelName)) continue;
			if(channelName.startsWith(String.format("rtcs_%s:", tableName)) || channelName.equals(String.format("rtcs_%s", tableName)) ){
				Set<Event> events = entry.getValue();
				for(Event e : events){
					if(e.isOnTable == isOnTableRef){
						ret.add(channelName);
						break;
					}
				}
			}
		}		
		return ret;		
	}
	
}
