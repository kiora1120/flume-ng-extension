import org.apache.flume.*;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.channel.PseudoTxnMemoryChannel;
import org.apache.flume.channel.ReplicatingChannelSelector;
import org.apache.flume.conf.Configurables;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * A class representing a window on the screen.
 * For example:
 * <pre>
 *    Window win = new Window(parent);
 *    win.show();
 * </pre>
 *
 * @author <A HREF="mailto:[kiora1120@gmail.com]">TJune Kim</A>
 * @version 1.0
 */
public class TestEventSourceTemplate {
    private Source source;
    private ChannelSelector selector;


    @Test
    public void basicTest() throws EventDeliveryException, InterruptedException {
        source = new EventSourceTemplate();

        Channel channel = new PseudoTxnMemoryChannel();
        Context context = new Context();
//        selector = ChannelSelectorFactory.create(channel, context);

        context.put("track", "문재인,안철수");
//        context.put("count", "5");

        Configurables.configure(source, context);
        Configurables.configure(channel, context);

        List<Channel> channels = new ArrayList<Channel>();
        channels.add(channel);

        ChannelSelector rcs = new ReplicatingChannelSelector();
        rcs.setChannels(channels);

        source.setChannelProcessor(new ChannelProcessor(rcs));
        source.start();
        for (long i = 0; i < 10000L; i++) {

            Event event = channel.take();
            if (event != null) {
                System.out.println(new String(event.getBody()));
                System.out.println(event.getHeaders());
            }

//            Assert.assertArrayEquals(String.valueOf(i).getBytes(),
//                    new String(event.getBody()).getBytes());
            Thread.sleep(1000);
        }

    }
}
