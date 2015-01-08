package co.realtime.storage;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import android.content.Context;

import com.fasterxml.jackson.databind.ObjectMapper;

import co.realtime.storage.Rest.RestType;
import co.realtime.storage.StorageRef.StorageEvent;
import co.realtime.storage.entities.Heartbeat;
import co.realtime.storage.entities.TableMetadata;
import co.realtime.storage.ext.OnError;
import co.realtime.storage.ext.OnPresence;
import co.realtime.storage.ext.OnTableMetadata;
import co.realtime.storage.ext.StorageException;
import ibt.ortc.api.Ortc;
import ibt.ortc.api.Presence;
import ibt.ortc.api.Strings;
import ibt.ortc.extensibility.OnConnected;
import ibt.ortc.extensibility.OnDisconnected;
import ibt.ortc.extensibility.OnException;
import ibt.ortc.extensibility.OnMessage;
import ibt.ortc.extensibility.OnReconnected;
import ibt.ortc.extensibility.OnReconnecting;
import ibt.ortc.extensibility.OnSubscribed;
import ibt.ortc.extensibility.OnUnsubscribed;
import ibt.ortc.extensibility.OrtcClient;
import ibt.ortc.extensibility.OrtcFactory;
import ibt.ortc.extensibility.exception.OrtcNotConnectedException;

class StorageContext {
	StorageRef storage;
	String applicationKey;
	@Deprecated
	String privateKey;
	String authenticationToken;
	boolean isCluster;
	boolean isSecure;
	String lastBalancerResponse;
	String url;
	String ortcUrl;
	public ObjectMapper mapper;

	public boolean bufferIsActive;
	private OrtcClient ortcClient;
	private HashMap<String, TableMetadata> metas;
	EventCollection evCollection;
	OnMessage onMessage;
	Map<String, Boolean> toSubscribe;
	Set<String> subscribeWithoutNotifications;
	Set<String> unsubscribing;
	ArrayList<Rest> offlineBuffer;
	boolean isOffline;

	private String googleProjectId;
	private Context androidApplicationContext;	

	private static String CHANNEL_REGEX_STRING = "rtcs_(.[^:]+)(:*)(.*)$";
	private static Pattern CHANNEL_PATTERN;

	static {
		CHANNEL_PATTERN = Pattern.compile(CHANNEL_REGEX_STRING);
	}

	co.realtime.storage.ext.OnReconnected onStorageReconnected = null;
	co.realtime.storage.ext.OnReconnecting onStorageReconnecting = null;
	
	// TODO: remove the 'privateKey' in the next release
	StorageContext(final StorageRef storage, 
			String applicationKey,
			String privateKey,
			String authenticationToken, 
			boolean isCluster, 
			boolean isSecure, 
			String url,
			Heartbeat heartbeat,
			String googleProjectId, 
			Context androidApplicationContext) throws StorageException {
		this.storage = storage;
		this.applicationKey = applicationKey;

		this.privateKey = privateKey == null ? null : Strings.isNullOrEmpty(privateKey) ? null : privateKey;
		
		// TODO: enable this check after the removing the private key
		//if(authenticationToken == null)
		//	throw new StorageException("The attribute 'authenticationToken' is mandatory.");
		
		this.authenticationToken = authenticationToken;
		this.isCluster = isCluster;
		this.isSecure = isSecure;
		if(!url.contains("http")) {
			this.url = isSecure ? "https://" + url : "http://" + url;	
			this.ortcUrl = "https://ortc-storage.realtime.co/server/ssl/2.1";
		}
		else {
			this.url = url;
			this.ortcUrl = "http://ortc-storage.realtime.co/server/2.1";
		}
		this.metas = new HashMap<String, TableMetadata>();
		this.lastBalancerResponse = null;
		this.evCollection = new EventCollection();
		this.toSubscribe = new HashMap<String,Boolean>();
		this.unsubscribing = new HashSet<String>();
		this.isOffline = false;
		this.offlineBuffer = new ArrayList<Rest>();
		this.subscribeWithoutNotifications = new HashSet<String>();

		this.googleProjectId = googleProjectId;
		this.androidApplicationContext = androidApplicationContext;

		bufferIsActive = true;
		mapper = new ObjectMapper();

		try {
			Ortc ortc = new Ortc();
			OrtcFactory factory = ortc.loadOrtcFactory("IbtRealtimeSJ");
			ortcClient = factory.createClient();
			
			if(isSecure){
				ortcClient.setClusterUrl(ortcUrl);
			} 
			else {
				ortcClient.setClusterUrl(ortcUrl);
			}			

			if(!Strings.isNullOrEmpty(this.googleProjectId)){
				ortcClient.setGoogleProjectId(this.googleProjectId);
			}

			if(this.androidApplicationContext != null){
				ortcClient.setApplicationContext(androidApplicationContext);
			}


			ortcClient.onException = new OnException(){
				public void run(OrtcClient ortcClient, Exception ex) {

				}
			};

			ortcClient.onReconnected = new OnReconnected(){
				public void run(OrtcClient oc) {
					//System.out.println("::reconnected");
					isOffline = false;
					if(onStorageReconnected != null)
						onStorageReconnected.run(storage);
					callRestFromBuffer();
				}

				private void callRestFromBuffer() {
					if(offlineBuffer.size()>0){
						Rest r = offlineBuffer.get(0);
						if(r!=null){
							offlineBuffer.remove(0);
							r.onRestCompleted = new OnRestCompleted(){
								@Override
								public void run() {
									callRestFromBuffer();							
								}
							};
							r.process();
						}
					}
				}				
			};

			ortcClient.onReconnecting = new OnReconnecting(){
				public void run(OrtcClient oc){
					//System.out.println("::reconecting");
					isOffline = true;
					if(onStorageReconnecting != null)
						onStorageReconnecting.run(storage);
				}
			};

			ortcClient.onConnected = new OnConnected(){
				public void run(OrtcClient oc) {
					//System.out.println("::connected");
					isOffline = false;
					for(String channel : toSubscribe.keySet()){
						Boolean withNotification = toSubscribe.get(channel);
						if(withNotification){
							//System.out.println("=> sub with notif: " + channel);
							ortcClient.subscribeWithNotifications(channel, true, onMessage);
						}else{
							//System.out.println("=> sub: " + channel);
							ortcClient.subscribe(channel, true, onMessage);
						}
					}
					//toSubscribe.clear();
				}				
			};

			ortcClient.onDisconnected = new OnDisconnected(){
				public void run(OrtcClient oc) {
					//System.out.println("::disconnected");
				}				
			};

			ortcClient.onException = new OnException(){
				public void run(OrtcClient oc, Exception ex) {
					ex.printStackTrace();
					//System.out.println(String.format("::exception: %s", ex.toString() ));
				}				
			};

			ortcClient.onSubscribed = new OnSubscribed(){
				@Override
				public void run(OrtcClient client, String channel) {
					//System.out.println(String.format(":: subscribed to %s", channel));
					toSubscribe.remove(channel);
				}				
			};

			ortcClient.onUnsubscribed = new OnUnsubscribed(){
				@Override
				public void run(OrtcClient client, String channel) {
					//System.out.println(String.format(":: unsubscribed from %s", channel));
					unsubscribing.remove(channel);
					if(toSubscribe.containsKey(channel)){
						Boolean withNotification = toSubscribe.get(channel);
						if(withNotification){
							//System.out.println("=> sub with notif: " + channel);
							ortcClient.subscribeWithNotifications(channel, true, onMessage);
						}else{
							//System.out.println("=> sub: " + channel);
							ortcClient.subscribe(channel, true, onMessage);
						}
					}else if(subscribeWithoutNotifications.contains(channel)){
						//System.out.println("=> sub: " + channel);
						ortcClient.subscribe(channel, true, onMessage);
					}
				}				
			};

			this.onMessage = new OnMessage(){
				@SuppressWarnings("unchecked")
				@Override
				public void run(OrtcClient client, final String channel, String messageJson) {
					//System.out.println(String.format(":: mess (%s): %s", channel, messageJson));
					Matcher matchResult = CHANNEL_PATTERN.matcher(channel);
					if(matchResult.matches()){
						final String tableName = matchResult.group(1);

						Map<String, Object> message;
						try {
							message = mapper.readValue(messageJson, Map.class);
						} catch (Exception e) {						
							e.printStackTrace();
							return;
						}
						final String type = (String) message.get("type");
						final LinkedHashMap<String, Object> item = (LinkedHashMap<String, Object>) message.get("data");
						//TableMetadata tm = getTableMeta(tableName);

						if(metas.containsKey(tableName)){
							parseNotificationMessage(tableName, type, item, channel);
						} else {
							storage.table(tableName).meta(new OnTableMetadata(){
								@Override
								public void run(TableMetadata tableMetadata) {
									parseNotificationMessage(tableName, type, item, channel);
								}}, new OnError(){
									@Override
									public void run(Integer code, String errorMessage) {
										//System.out.println("Error#: " + errorMessage);
									}});
						}
					}
				}				
			};

			if(heartbeat != null) {
				setHeartbeat(heartbeat);
			}
			
			ortcClient.connect(applicationKey, authenticationToken);

		} catch (Exception e) {
			throw new StorageException(e.toString());
		}	
	}

	void parseNotificationMessage(String tableName, String type, LinkedHashMap<String, Object> item, String channelName){
		TableMetadata tm = getTableMeta(tableName);
		LinkedHashMap<String, ItemAttribute> itemMap = null;
		ItemSnapshot itemSnapshot = null;
		if(item!=null) {
			itemMap = ProcessRestResponse.convertItemMap(item);
			ItemAttribute primary = itemMap.get(tm.getPrimaryKeyName());
			String secondaryKeyName = tm.getSecondaryKeyName();
			ItemAttribute secondary = null;
			if(secondaryKeyName != null)
				secondary = itemMap.get(secondaryKeyName);
			itemSnapshot = new ItemSnapshot(storage.table(tableName), itemMap, primary, secondary);
		} else {
			itemSnapshot = new ItemSnapshot(storage.table(tableName), null, null, null);
		}
		//Boolean unsubscribe = evCollection.fireEvents(tableName, StorageEvent.fromString(type), primary, secondary, is);
		Boolean unsubscribe = evCollection.fireEvents(channelName, StorageEvent.fromString(type), itemSnapshot);
		if(unsubscribe){
			//String channelName = String.format("rtcs_%s", tableName);
			if(!this.unsubscribing.contains(channelName)){
				ortcClient.unsubscribe(channelName);
				this.unsubscribing.add(channelName);
			}
		}
	}

	void setOnReconnected(co.realtime.storage.ext.OnReconnected callback, StorageRef storage){
		this.storage = storage;
		this.onStorageReconnected = callback;
	}

	void setOnReconnecting(co.realtime.storage.ext.OnReconnecting callback, StorageRef storage){
		this.storage = storage;
		this.onStorageReconnecting = callback;
	}

	void processRest(Rest r){
		if(this.isOffline){
			if((r.type == RestType.PUTITEM || r.type == RestType.UPDATEITEM || r.type == RestType.DELETEITEM) && this.bufferIsActive){
				this.offlineBuffer.add(r);
			} else {
				if(r.onError != null){
					r.onError.run(1007, "Can not establish connection with storage!");
				}
			}
		} else {
			r.process();
		}
	}

	void addTableMeta(TableMetadata tm){
		String name = tm.getName();
		metas.put(name, tm);
	}

	TableMetadata getTableMeta(String name){
		return metas.get(name);
	}
	
	public void presence(String channel, final OnPresence onPresence, final OnError onError) {		
		ibt.ortc.api.OnPresence onOrtcPresence = new ibt.ortc.api.OnPresence() {
			@Override
			public void run(Exception error, Presence presence) {
				if(error == null) {
					onPresence.run(presence);
				}
				else {
					onError.run(1011, error.getMessage());						
				}				
			}
		};
		
		try {
			if(ortcClient.getIsConnected()) {
				ortcClient.presence(channel, onOrtcPresence);
			}
			else {
				Ortc.presence(ortcUrl, isCluster, applicationKey, authenticationToken, channel, onOrtcPresence);
			}
		} 
		catch (OrtcNotConnectedException e) {
			onError.run(1008, e.getMessage());	
		}
	}
	
	public void setHeartbeat(Heartbeat heartbeat) {
		if(heartbeat.getActive() != null) {
			ortcClient.setHeartbeatActive(heartbeat.getActive());
		}
		
		if(heartbeat.getFails() != null) {
			ortcClient.setHeartbeatFails(heartbeat.getFails());
		}
		
		if(heartbeat.getTime() != null) {
			ortcClient.setHeartbeatTime(heartbeat.getTime());
		}
	}
	
	public Heartbeat getHeartbeat() throws StorageException {
		return new Heartbeat(ortcClient.getHeartbeatActive(),
				ortcClient.getHeartbeatFails(),
				ortcClient.getHeartbeatTime());
	}
	
	public void addEvent(Event ev) {
		if(ev.onItemSnapshot == null) return;
		Boolean doSubscription = evCollection.add(ev);
		if(doSubscription){
			//String channelName = String.format("rtcs_%s", ev.tableName);
			String channelName = ev.getChannelName();
			if(ortcClient.getIsConnected() && !this.unsubscribing.contains(channelName)){				
				if(ev.pushNotificationsEnabled){
					//System.out.println("=> sub with notif: " + channelName);
					ortcClient.subscribeWithNotifications(channelName, true, this.onMessage);
				}else{
					//System.out.println("=> sub: " + channelName);
					ortcClient.subscribe(channelName, true, this.onMessage);
				}
			} else {
				this.toSubscribe.put(channelName,ev.pushNotificationsEnabled);
			}
		}
	}

	public void removeEvent(Event ev) {
		Boolean unsubscribe = evCollection.remove(ev);
		if(unsubscribe){
			//String channelName = String.format("rtcs_%s", ev.tableName);
			String channelName = ev.getChannelName();
			if(!this.unsubscribing.contains(channelName)){
				ortcClient.unsubscribe(channelName);
				this.unsubscribing.add(channelName);
			}
		}
	}

	private void unsubscribeAllNotifications(String channelName){
		if(ortcClient.isSubscribed(channelName)){
			subscribeWithoutNotifications.add(channelName);
			ortcClient.unsubscribe(channelName);
		}
	}

	public void disablePushNotifications(String table){
		String channelName = String.format("rtcs_%s", table);     
		unsubscribeAllNotifications(channelName);
	}

	public void disablePushNotifications(String table, String primaryKey){
		String channelName = String.format("rtcs_%s:%s", table,primaryKey);     
		unsubscribeAllNotifications(channelName);
	}

	public void disablePushNotifications(String table, String primaryKey, String secondaryKey){
		String channelName = String.format("rtcs_%s:%s_%s", table,primaryKey,secondaryKey);     
		unsubscribeAllNotifications(channelName);
	}

	public void disablePushNotificationsForChannels(ArrayList<String> channels) {
		for(String channelName : channels){
			unsubscribeAllNotifications(channelName);
		}
	}

	public void disablePushNotificationsForChannels(String tableName) {
		ArrayList<String> channels = evCollection.getChannelNames(tableName, true);
		disablePushNotificationsForChannels(channels);
	}
}
