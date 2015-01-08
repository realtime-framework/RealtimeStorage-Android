package co.realtime.storage;


import ibt.ortc.api.OnRestWebserviceResponse;
import ibt.ortc.api.Strings;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;

class RestWebservice {
	
	protected static void getAsync(URL url,OnRestWebserviceResponse callback){
		requestAsync(url,"GET", null, callback);
	}
	
	protected static void postAsync(URL url,String content,OnRestWebserviceResponse callback){
		requestAsync(url,"POST", content, callback);
	}
	
	private static void requestAsync(final URL url,final String method, final String content,final OnRestWebserviceResponse callback){
		Runnable task = new Runnable() {
			
			@Override
			public void run() {
				if(method.equals("GET")){
					try {
						String result = "https".equals(url.getProtocol()) ? secureGetRequest(url) : unsecureGetRequest(url);
						
						callback.run(null, result);
					} catch (Exception error) {
						
						callback.run(error, null);						
					}
				}else if(method.equals("POST")){
					String result = null;
					try {
						result = "https".equals(url.getProtocol()) ? securePostRequest(url,content) : unsecurePostRequest(url,content);
					} catch (Exception e) {
						callback.run(e, null);
					}
					if(result != null)
						callback.run(null, result);
				}else{					
					callback.run(new Exception("Invalid request method - " + method), null);
				}
			}
		};
		
		new Thread(task).start();		
	}
	
	private static String unsecureGetRequest(URL url) throws Exception {
		HttpURLConnection connection = (HttpURLConnection) url.openConnection();		
		
		//connection.setDoOutput(true);

		String result = "";
		
		InputStream responseBody = null;
		if(connection.getResponseCode() != 200 && connection.getResponseCode() != -1){			
			responseBody = connection.getErrorStream();			
			
			result = readResponseBody(responseBody);
			
			throw new Exception(result);
		}else{
			responseBody = connection.getInputStream();
			if(responseBody == null){
				responseBody = connection.getErrorStream();				
				
				result = readResponseBody(responseBody);
				
				throw new Exception(result);
			}else{
				result = readResponseBody(responseBody);
			}
		}

		return result;
	}
	
	private static String secureGetRequest(URL url) throws Exception {
		HttpsURLConnection connection = (HttpsURLConnection) url.openConnection();
		
		//connection.setDoOutput(true);

		String result = "";
		
		if(connection.getResponseCode() != 200){
			BufferedReader rd = new BufferedReader(new InputStreamReader(connection.getErrorStream()));
			String line;
			while ((line = rd.readLine()) != null) {
				result += line;
			}
			rd.close();
			
			throw new Exception(result);
		}else{
			BufferedReader rd = new BufferedReader(new InputStreamReader(connection.getInputStream()));
			String line;
			while ((line = rd.readLine()) != null) {
				result += line;
			}
			rd.close();
		}

		return result;
	}	

	private static String unsecurePostRequest(URL url, String postBody) throws Exception {
		HttpURLConnection connection = (HttpURLConnection) url.openConnection();		
		connection.setRequestMethod("POST");
		connection.setRequestProperty("Content-Type", "application/json");
		connection.setRequestProperty("Accept", "application/json");
		connection.setDoOutput(true);
		OutputStreamWriter wr = new OutputStreamWriter(connection.getOutputStream());

		wr.write(postBody);

		wr.flush();
		
		String result = "";
		
		if(connection.getResponseCode() != 200){
			BufferedReader rd = new BufferedReader(new InputStreamReader(connection.getErrorStream()));
			String line;
			while ((line = rd.readLine()) != null) {
				result += line;
			}
			rd.close();
			
			throw new Exception(result);
		}else{
			BufferedReader rd = new BufferedReader(new InputStreamReader(connection.getInputStream()));
			String line;
			while ((line = rd.readLine()) != null) {
				result += line;
			}
			rd.close();
		}
		wr.close();

		return result;
	}
	
	private static String securePostRequest(URL url, String postBody) throws Exception {		
		
		//URL url = new URL("https://ortc-mobilepush.realtime.co/mp/topicdevices/YOUR_CHANNEL");
		//String postBody = "{\"applicationKey\": \"YOUR_APP_KEY\", \"privateKey\": \"YOUR_PRV_KEY\"}";
		
		HttpsURLConnection conn = (HttpsURLConnection) url.openConnection();

		SSLContext sc;
		sc = SSLContext.getInstance("TLS");	
		sc.init(null, null, new java.security.SecureRandom());

		conn.setSSLSocketFactory(sc.getSocketFactory());
		conn.setRequestMethod("POST");
		conn.setDoInput(true);
		conn.setRequestProperty("Content-Type", "application/json");

		DataOutputStream wr = new DataOutputStream(conn.getOutputStream());
		wr.writeBytes(postBody);
		wr.flush();
		wr.close();

		conn.connect();
		BufferedReader stdIn = new BufferedReader(new InputStreamReader(conn.getInputStream()));

		StringBuffer response = new StringBuffer();
		String line;
		while ((line = stdIn.readLine()) != null) {
			response.append(line);
		}
		stdIn.close();
		return response.toString();
		
		/*
		SSLSocketFactory sslsocketfactory =  (SSLSocketFactory) SSLSocketFactory.getDefault();			
		SSLSocket sslsocket = (SSLSocket) sslsocketfactory.createSocket(url.getHost(), url.getPort());				
		sslsocket.setEnabledProtocols(new String[] {"SSLv3"});
		
		OutputStream outputstream = sslsocket.getOutputStream();
		OutputStreamWriter outputstreamwriter = new OutputStreamWriter(outputstream);
		BufferedWriter bufferedwriter = new BufferedWriter(outputstreamwriter);

		BufferedReader stdIn = new BufferedReader(new InputStreamReader(sslsocket.getInputStream()));
		
		String request = String.format("POST %s HTTP/1.1\r\n",url.getFile()) + 
						"Content-Type: application/json\r\n" +
						"Content-Length: "+String.valueOf(postBody.length())+"\r\n\r\n" + postBody;
		
		bufferedwriter.write(request);
		bufferedwriter.flush();
		
		String response = "";
		String line;
		int responseLenght = 0;
		while ((line = stdIn.readLine()) != null) {
			if(line.startsWith("Content-Length")){
				String l = line.replaceAll("Content-Length: ", "");
				responseLenght = Integer.parseInt(l);
			}
			if(Strings.isNullOrEmpty(line)) break;//headerEnded = true;
		}
		if(responseLenght >0){
			char[] buffer = new char[responseLenght];
			stdIn.read(buffer, 0, responseLenght);
			response = new String(buffer);			
		}
		outputstream.close();
		stdIn.close();
		sslsocket.close();
		bufferedwriter.close();
		
		return response;
		*/
	}
	
	
	private static String readResponseBody(InputStream responseBody){
		String result = "";
		
		if(responseBody != null){
			BufferedReader rd = new BufferedReader(new InputStreamReader(responseBody));
			String line;
			try {
				while ((line = rd.readLine()) != null) {
					result += line;
				}
				rd.close();
			} catch (Exception e) { 
				result = e.getMessage();
			} 
		}
		
		return result;
	}
}
