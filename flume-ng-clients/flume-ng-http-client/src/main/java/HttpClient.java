import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;

/**
 * Http Client.
 *
 * </p>
 *
 * Apache Flume의 HttpSource로 Event를 보낸다.
 *
 * @author <a href="mailto:jhshin9@gmail.com">신정훈</a>
 * @since 1.0
 */
public class HttpClient {

	public static void main(String[] args) {

		try {

			DefaultHttpClient httpClient = new DefaultHttpClient();
			HttpPost postRequest = new HttpPost("http://localhost:5140");

			StringEntity input = new StringEntity("[{\n" +
				"\"headers\" : {\n" +
				"\"systemId\" : \"Flume\",\n" +
				"\"host\" : \"www.jbug.org\"\n" +
				"},\n" +
				"\"body\" : \"{data : aaaaaaaaaaaa\"\n" +
				"}]");
			input.setContentType("application/json");
			postRequest.setEntity(input);

			HttpResponse response = httpClient.execute(postRequest);

			if (response.getStatusLine().getStatusCode() != 201 || response.getStatusLine().getStatusCode() != 200) {
				throw new RuntimeException("Failed : HTTP error code : " + response.getStatusLine().getStatusCode());
			}

			BufferedReader br = new BufferedReader(new InputStreamReader((response.getEntity().getContent())));

			String output;
			System.out.println("Output from Server .... \n");
			while ((output = br.readLine()) != null) {
				System.out.println(output);
			}
			httpClient.getConnectionManager().shutdown();
		} catch (MalformedURLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
