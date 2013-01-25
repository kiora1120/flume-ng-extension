import org.apache.flume.*;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.channel.PseudoTxnMemoryChannel;
import org.apache.flume.channel.ReplicatingChannelSelector;
import org.apache.flume.conf.Configurables;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * InfinispanEventDrivenSource Test Case.
 * </p>
 *
 * @author <a href="mailto:jhshin9@gmail.com">신정훈</a>
 * @version 1.0
 * @since 1.0
 */
public class InfinispanEventDrivenSourceTest {

    /**
     * SLF4J Logging
     */
    private Logger logger = LoggerFactory.getLogger(this.getClass());

    private Source source;

    Channel channel;

    @Before
    public void setUp() {

        source = new InfinispanEventDrivenSource();
        channel = new PseudoTxnMemoryChannel();

        Context context = new Context();
        context.put("configPath", "config.xml");
        context.put("cacheName", "sharedCache");

        Configurables.configure(source, context);
        Configurables.configure(channel, context);
    }


    @Test
    public void infinispanSourceTest() throws EventDeliveryException, InterruptedException {

        List<Channel> channels = new ArrayList<Channel>();
        channels.add(channel);

        ChannelSelector channelSelector = new ReplicatingChannelSelector();
        channelSelector.setChannels(channels);

        source.setChannelProcessor(new ChannelProcessor(channelSelector));

        source.start();

        for (long i = 0; i < 10000L; i++) {
            Event event = channel.take();
            if (event != null) {
                logger.debug("Event Body : [{}]", new String(event.getBody()));
                //logger.debug(event.getHeaders());
            }

            Thread.sleep(1000);
        }
    }
}
