import org.apache.flume.Context;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TwitterStreamSource
 *
 * @author <A HREF="mailto:[kiora1120@gmail.com]">TJune Kim</A>
 * @version 1.0
 */
public class PollSourceTemplate extends AbstractSource implements PollableSource, Configurable {


    private String name = "";


    @Override
    public void start() {
        super.start();
    }

    @Override
    public void stop() {
        super.stop();
    }

    @Override
    public void configure(Context context) {
        name = context.getString("name", "");
    }

    @Override
    public Status process() throws EventDeliveryException {
        try {
            getChannelProcessor().processEvent(EventBuilder.withBody((name + ":" + System.currentTimeMillis()).getBytes()));
        } catch (Exception e) {
            e.printStackTrace();
            return Status.BACKOFF;
        }


        return Status.READY;
    }
}
