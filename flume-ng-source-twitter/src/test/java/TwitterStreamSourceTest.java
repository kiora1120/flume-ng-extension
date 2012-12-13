import org.apache.flume.*;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.channel.ChannelSelectorFactory;
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
public class TwitterStreamSourceTest {
    private Source source;

    private ChannelSelector selector;


    @Test
    public void streamTwitter() throws EventDeliveryException, InterruptedException {
        source = new TwitterStreamSource();

        Channel channel = new PseudoTxnMemoryChannel();
        Context context = new Context();
//        selector = ChannelSelectorFactory.create(channel, context);

        context.put("track", "문재인,안철수");
//        context.put("count", "5");
        context.put("consumerKey","8GQWCNJtBgyvYdwpoUTgA");
        context.put("consumerSecret","K2eYATNj440tT9s7Lt6TwJnluyqDhXPdu6ZYRdwxRsE");
        context.put("accessToken","164970128-W1AhGjeQvSM7poPW351S518ImQW44pBsIwymo8Ns");
        context.put("accessTokenSecret","HV7Y5f5wdfFzePSQWVVcuMc3mylqq3U7cD9hjxrn4");

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

    @Test
    public void makeLocationsDoubleArrayTest(){
        String locations = "11.22,123.44|22.33,2314.11";

        System.out.println(TwitterStreamSource.makeLocationsDoubleArray(locations, "\\|"));


    }
}
