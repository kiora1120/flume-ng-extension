import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Jboss Infinispan Sink.
 * </p>
 * <p/>
 * Put data into Infinispan Data Grid.
 *
 * @author <a href="mailto:jhshin9@gmail.com">Junghun, Shin (Korea)</a>
 * @version 1.0
 * @since 1.0
 */
public class InfinispanSink extends AbstractSink implements Configurable {

    /**
     * SLF4J Logging
     */
    private Logger logger = LoggerFactory.getLogger(this.getClass());

    private String nodes;

    private String cacheName;

    RemoteCacheManager remoteCacheManager;

    RemoteCache remoteCache;

    @Override
    public void configure(Context context) {
        nodes = context.getString("nodes", "localhost:11222");
        cacheName = context.getString("cacheName", "localCache");
    }

    @Override
    public synchronized void start() {
        logger.info("InfinispanSink is started.............");
        remoteCacheManager = new RemoteCacheManager(nodes);
        remoteCache = remoteCacheManager.getCache(cacheName);
        super.start();
    }

    @Override
    public Status process() throws EventDeliveryException {

        Status status = Status.READY;
        Channel channel = getChannel();
        Transaction tx = null;

        try {
            tx = channel.getTransaction();
            tx.begin();

            Event event = channel.take();

            if (event != null) {
                byte[] body = event.getBody();
                String data = new String(body);

                // Put the Event into Infinispan Cache
                remoteCache.put(event.hashCode(), data);

            } else {
                status = Status.BACKOFF;
            }

            tx.commit();
        } catch (Exception e) {
            logger.error("can't process events, drop it!", e);
            if (tx != null) {
                tx.commit();
            }
            throw new EventDeliveryException(e);
        } finally {
            if (tx != null) {
                tx.close();
            }
        }
        return status;
    }

    @Override
    public synchronized void stop() {
        logger.info("InfinispanSink is stopped.............");
        remoteCache.stop();
        remoteCacheManager.stop();
        super.stop();
    }
}
