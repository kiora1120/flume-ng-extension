import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TwitterStreamSource
 *
 * @author <A HREF="mailto:[kiora1120@gmail.com]">TJune Kim</A>
 * @version 1.0
 */
public class EventSourceTemplate extends AbstractSource implements Configurable, EventDrivenSource {
    private static final Logger logger = LoggerFactory.getLogger(EventSourceTemplate.class);

    private String name;


    @Override
    public void configure(Context context) {
        logger.info("Configure {}...", context.getString("name", ""));

        name = context.getString("name");
    }

    @Override
    public void start() {
        logger.info("Starting {}...", this);
        super.start();
        logger.info("Tweet source {} started.", getName());
    }

    @Override
    public void stop() {
        logger.info("Avro source {} stopping: {}", getName(), this);
        super.stop();
    }



}
