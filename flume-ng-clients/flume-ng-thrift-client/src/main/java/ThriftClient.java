import org.apache.log4j.Logger;
import org.apache.log4j.Priority;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import java.nio.ByteBuffer;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;

/**
 * Apache Thrift Client.
 *
 * </p>
 *
 *  Apache Flume의 Thrift Source로 Event를 보낸다.
 *
 * @author <a href="mailto:jhshin9@gmail.com">신정훈</a>
 * @since 1.0
 */
public class ThriftClient {

/*
	public static void main(String args[]) {
	        *//*ThriftFlumeEvent tfe = new ThriftFlumeEvent();
	        Map<String, ByteBuffer> fields = new HashMap<String, ByteBuffer>();
	        fields.put("topic", ByteBuffer.wrap("test".getBytes()));

	        tfe.fields = fields;
	        tfe.priority = Priority.INFO;
	        tfe.timestamp = new Date().getTime();
	        tfe.host = "localhost";

	        tfe.body = ByteBuffer.wrap("test body".getBytes());
	        Client client = getClient();
	        try {
	            client.append(tfe);
	        } catch (TException ex) {
	            Logger.getLogger(FlumeThriftTest.class.getName()).log(Level.SEVERE, null, ex);
	        } finally {
	            try {
	                client.close();
	            } catch (TException ex) {
	                Logger.getLogger(FlumeThriftTest.class.getName()).log(Level.SEVERE, null, ex);
	            }
	        }

	    }

	    public static Client getClient() {
	        TTransport transport = new TSocket("localhost", 10400);
	        if (!transport.isOpen()) {
	            try {
	                transport.open();
	            } catch (Exception e) {
	                e.printStackTrace();
	            }
	        }

	        TProtocol protocol = new TBinaryProtocol(transport);
	       *//* return new Client(protocol);
	    }*/
}
