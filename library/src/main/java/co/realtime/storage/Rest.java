package co.realtime.storage;

import ibt.ortc.api.OnRestWebserviceResponse;
import ibt.ortc.api.SecureWebConnections;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.UnknownHostException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;

import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import co.realtime.storage.StorageRef.StorageOrder;
import co.realtime.storage.entities.TableMetadata;
import co.realtime.storage.ext.OnBooleanResponse;
import co.realtime.storage.ext.OnError;
import co.realtime.storage.ext.OnItemSnapshot;
import co.realtime.storage.ext.OnTableCreation;
import co.realtime.storage.ext.OnTableMetadata;
import co.realtime.storage.ext.OnTableSnapshot;
import co.realtime.storage.ext.OnTableUpdate;
import co.realtime.storage.ext.StorageException;


class Rest {
	enum RestType {
		ISAUTHENTICATED ("isAuthenticated"), LISTITEMS ("listItems"), QUERYITEMS ("queryItems"), GETITEM ("getItem"), PUTITEM ("putItem"),
		UPDATEITEM ("updateItem"), DELETEITEM ("deleteItem"), CREATETABLE ("createTable"), UPDATETABLE ("updateTable"), DELETETABLE ("deleteTable"),
		LISTTABLES ("listTables"), DESCRIBETABLE ("describeTable"), INCR ("incr"),  DECR("decr");

		private final String restName;

		private RestType(String s){
			restName = s;
		}

		public String toString() {
			return restName;
		}
	};

	StorageContext context;
	RestType type;
	TableRef table;
	URL requestUrl;
	PostBodyBuilder bodyBuilder;
	private LinkedHashMap<String, Object> lastStopKey;
    private String lastStopTable;
	private ArrayList<LinkedHashMap<String, Object>> allItems;

	public OnError onError = null;
	public OnTableSnapshot onTableSnapshot = null;
	public OnBooleanResponse onBooleanResponse = null;
	public OnItemSnapshot onItemSnapshot = null;
	public OnTableMetadata onTableMetadata = null;
	public OnTableCreation onTableCreation = null;
	public OnTableUpdate onTableUpdate = null;
	OnRestCompleted onRestCompleted = null;
	public String rawBody = null;
	public StorageOrder order = StorageOrder.NULL;
	public Long limit = null;
	public boolean endWithNull = false;

	Rest(StorageContext context, RestType type, PostBodyBuilder bodyBuilder, TableRef table){
		this.context = context;
		this.type  = type;
		this.bodyBuilder = bodyBuilder;
		this.table = table;
		this.requestUrl = null;
		this.lastStopKey = null;
        this.lastStopTable = null;
		this.allItems = new ArrayList<LinkedHashMap<String, Object>>();
		this.limit = (Long) bodyBuilder.getObject("limit");
	}

	void process(){
		final Rest that = this;
		new Thread(new Runnable() {
			@Override
			public void run() {

				try {
					resolveUrl();
				} catch (Exception e) {
					if(that.onError!=null)
						that.onError.run(1002, e.getMessage());
					return;
				}




				if(that.requestUrl==null){
					that.context.lastBalancerResponse = null;
					if(that.onError!=null)
						that.onError.run(1003, "Can not get response from balancer!");
					return;
				}
				if(lastStopKey!=null)
					bodyBuilder.addObject("startKey", lastStopKey);

                if(lastStopTable!=null){
                    bodyBuilder.addObject("startTable", lastStopTable);
                }
				String rBody;
				try {
					rBody = (that.rawBody!=null?that.rawBody:that.bodyBuilder.getBody());
				} catch (JsonProcessingException e) {
					if(that.onError!=null)
						that.onError.run(1004, e.getMessage());
					return;
				}
				
				//System.out.println("[-] request: ("+that.type.toString()+") " + rBody);
				
				RestWebservice.postAsync(that.requestUrl, rBody, new OnRestWebserviceResponse(){
					@SuppressWarnings("unchecked")
					@Override
					public void run(Exception e, String r) {
						if(onRestCompleted != null)
							onRestCompleted.run();
						if(e!=null){
							if(context.isCluster && context.lastBalancerResponse!=null){
								context.lastBalancerResponse = null;
								process();
							} else {
								if(onError != null)
									onError.run(1005, e.getMessage());
							}
						} else {
							
							//System.out.println(String.format("[-] response %s", r));
							
							ObjectMapper mapper = new ObjectMapper();						

							Map<String, Object> data;
							try {
								data = mapper.readValue(r, Map.class);
							} catch (Exception ex) {					
								if(onError != null){
									//System.out.println(String.format("::response %s", r));
									onError.run(1006, ex.getMessage());
								}
								return;
							}
							LinkedHashMap<String, Object> error = (LinkedHashMap<String, Object>)data.get("error");
							if(error != null){
								if(onError!=null)
									onError.run((Integer)error.get("code"), (String)error.get("message"));
							}else{
								if(type==RestType.LISTITEMS || type==RestType.QUERYITEMS){
									LinkedHashMap<String, Object> rdata = (LinkedHashMap<String, Object>)data.get("data");
									LinkedHashMap<String, Object> stopKey = (LinkedHashMap<String, Object>)rdata.get("stopKey");
									ArrayList<LinkedHashMap<String, Object>> items = (ArrayList<LinkedHashMap<String, Object>>)rdata.get("items");
									allItems.addAll(items);

									if((type!=RestType.QUERYITEMS || (limit!=null && limit>allItems.size())) && stopKey!=null ){
										lastStopKey = stopKey;
										process();
										return;									
									}
								}

                                if(type == RestType.LISTTABLES){
                                    LinkedHashMap<String, Object> rData = (LinkedHashMap<String, Object>)data.get("data");
                                    String stopTable = (String) rData.get("stopTable");
                                    ArrayList<String> tables = (ArrayList<String>) rData.get("tables");
                                    if(!stopTable.isEmpty() && tables.isEmpty()){
                                        lastStopTable = stopTable;
                                        process();
                                        return;
                                    }
                                }

								switch(type){
								case LISTITEMS:
									String sortKey = null;
									if(order != StorageOrder.NULL){
										TableMetadata tm = context.getTableMeta(table.name());
										sortKey = tm.getSecondaryKeyName();
										if(sortKey == null)
											sortKey = tm.getPrimaryKeyName();
									}
									ProcessRestResponse.processListItems(allItems, table, onItemSnapshot, order, sortKey, limit);
									break;
								case ISAUTHENTICATED: ProcessRestResponse.processIsAuthenticated(data, onBooleanResponse); break;								
								case QUERYITEMS: ProcessRestResponse.processQueryItems(allItems, table, onItemSnapshot); break;									
								case GETITEM: ProcessRestResponse.processGetItem(data, table, onItemSnapshot, endWithNull); break;
								case PUTITEM: ProcessRestResponse.processPutItem(data, table, onItemSnapshot); break;
								case UPDATEITEM: ProcessRestResponse.processUpdateItem(data, table, onItemSnapshot);break;
								case DELETEITEM: ProcessRestResponse.processDelItem(data, table, onItemSnapshot); break;
								case CREATETABLE: ProcessRestResponse.processCreateTable(data, onTableCreation); break;									
								case UPDATETABLE: ProcessRestResponse.processUpdateTable(data, onTableUpdate);	break;
								case DELETETABLE: ProcessRestResponse.processDeleteTable(data, onBooleanResponse); break;
								case LISTTABLES: ProcessRestResponse.processListTables(data, context, onTableSnapshot); break;
								case DESCRIBETABLE: ProcessRestResponse.processDescribeTable(data, context, onTableMetadata); break;
								case INCR:
								case DECR: ProcessRestResponse.processInDeCrResponse(data, table, onItemSnapshot); break;
								}
							}
						}
					}
				});

			}}).start();

	}

	//will put the server url with rest path to this.requestUrl
	void resolveUrl() throws IOException, StorageException, KeyManagementException, NoSuchAlgorithmException{
		String tempUrl;
		if(context.isCluster){
			if(this.context.lastBalancerResponse!=null){
				tempUrl = this.context.lastBalancerResponse;				
			} else {
				//System.out.println("[-] Balancer resolve! " + this.context);
				String urlString = context.url + "?appkey=" + context.applicationKey;
				URL url = new URL(urlString);
				String balancerResponse = urlString.startsWith("https:") ? secureBalancerRequest(url) : unsecureBalancerRequest(url);
				if(balancerResponse == null){
					throw new StorageException("Cannot get response from balancer!");
				}
				JSONObject obj = (JSONObject) JSONValue.parse(balancerResponse);
				tempUrl = (String)obj.get("url");
				this.context.lastBalancerResponse = tempUrl;
			}
		} else {
			tempUrl = context.url;
		}
		//tempUrl = "https://storage-ssl-prd-useast1-s0002.realtime.co:443/";
		//this.requestUrl += this.requestUrl.substring(this.requestUrl.length() - 1).equals("/") ? this.type.toString() : "/"+this.type.toString();
		tempUrl += tempUrl.substring(tempUrl.length() - 1).equals("/") ? this.type.toString() : "/"+this.type.toString();
		//path += path.substring(path.length(), 1).equals("/") ? this.type.toString() : "/"+this.type.toString();
		this.requestUrl = new URL(tempUrl);
		//System.out.println(String.format("Rest url: %s",this.requestUrl));
	}


	private String unsecureBalancerRequest(URL url) throws IOException {
		HttpURLConnection connection = (HttpURLConnection) url.openConnection();
		connection.setRequestProperty("user-agent", "storage-java-client");
		BufferedReader rd = new BufferedReader(new InputStreamReader(connection.getInputStream()));
		String result = "";
		String line;
		while ((line = rd.readLine()) != null) {
			result += line;
		}
		rd.close();
		return result;
	}

	private String secureBalancerRequest(URL url) throws UnknownHostException, IOException, NoSuchAlgorithmException, KeyManagementException {		
		int port = url.getPort() == -1 ? 443 : url.getPort();


		HttpsURLConnection conn = (HttpsURLConnection) url.openConnection();

		SSLContext sc;
		sc = SSLContext.getInstance("TLS");	
		sc.init(null, null, new java.security.SecureRandom());

		conn.setSSLSocketFactory(sc.getSocketFactory());
		conn.setRequestMethod("GET");
		conn.setDoInput(true);

		conn.connect();
		BufferedReader stdInBr = new BufferedReader(new InputStreamReader(conn.getInputStream()));

		StringBuffer response = new StringBuffer();
		String iline;
		while ((iline = stdInBr.readLine()) != null) {
			response.append(iline);
		}
		stdInBr.close();
		
		
		return response.toString();
		
		
		
		
		
		
		
		/*
		SSLSocketFactory sslsocketfactory = SecureWebConnections.getFullTrustSSLFactory();			
		SSLSocket sslsocket = (SSLSocket) sslsocketfactory.createSocket(url.getHost(), port);		
		String[] protocols = { "SSLv3" };
		sslsocket.setEnabledProtocols(protocols);

		OutputStream outputstream = sslsocket.getOutputStream();
		OutputStreamWriter outputstreamwriter = new OutputStreamWriter(outputstream);
		BufferedWriter bufferedwriter = new BufferedWriter(outputstreamwriter);

		BufferedReader stdIn = new BufferedReader(new InputStreamReader(sslsocket.getInputStream()));

		String request = String.format("GET %s HTTP/1.1\r\n",url.getFile()) + 
				"User-Agent: storage-java-client\r\n" + 
				"Connection: keep-alive\r\n" + 
				"Accept: * / *\r\n" +
				"host: " + url.getHost() + "\r\n" + 
				"Accept-Encoding: gzip,deflate\r\n" + 
				"Accept-Language: en-us\r\n\r\n";

		bufferedwriter.write(request);
		bufferedwriter.flush();

		String server = null;
		String line;
		while (server == null && (line = stdIn.readLine()) != null) {
			if(line.startsWith("{\"url\":")){
				server = line;
			}
		}
		outputstream.close();
		stdIn.close();
		sslsocket.close();
		return server;
		*/
	}	
}
