import org.jgroups.*;

/**
 * Description.
 *
 * </p>
 *
 * @author <a href="mailto:jhshin9@gmail.com">신정훈</a>
 * @since 1.0
 */
public class JgroupsUdpClient {

	public static void main(String[] args) throws ChannelException {

		JChannel channel = new JChannel("udp.xml");
		channel.setReceiver(new ReceiverAdapter() {
			public void receive(Message msg) {
				System.out.println("received msg from " + msg.getSrc() + ": " + msg.getObject());
			}
		});
		channel.connect("aaa");
		channel.send(new Message(null, null, "hello world"));
		channel.close();
	}
}
