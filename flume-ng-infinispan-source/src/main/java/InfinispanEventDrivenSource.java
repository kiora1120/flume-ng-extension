import org.apache.flume.Context;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.source.AbstractSource;
import org.infinispan.Cache;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryCreated;
import org.infinispan.notifications.cachelistener.annotation.CacheEntryRemoved;
import org.infinispan.notifications.cachelistener.event.CacheEntryCreatedEvent;
import org.infinispan.notifications.cachelistener.event.CacheEntryRemovedEvent;
import org.infinispan.notifications.cachemanagerlistener.annotation.CacheStarted;
import org.infinispan.notifications.cachemanagerlistener.annotation.CacheStopped;
import org.infinispan.notifications.cachemanagerlistener.event.CacheStartedEvent;
import org.infinispan.notifications.cachemanagerlistener.event.CacheStoppedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Infinispan Source.
 * <p/>
 * <p/>
 * Only for LocalCache not RemoteCache
 * </p>
 *
 * @author <a href="mailto:jhshin9@gmail.com">신정훈</a>
 * @see @see <a href="https://community.jboss.org/wiki/DesignOfRemoteEventHandlingInHotRod?_sscc=t">Remote Listener</a>
 * @since 1.0
 */
public class InfinispanEventDrivenSource extends AbstractSource implements Configurable, EventDrivenSource {

    /**
     * SLF4J Logging
     */
    private Logger logger = LoggerFactory.getLogger(this.getClass());

    private String configPath;

    private String cacheName;

    DefaultCacheManager cacheManager;

    Cache cache;

    @Override
    public void configure(Context context) {
        configPath = context.getString("configPath", "config.xml");
        cacheName = context.getString("cacheName", "sharedCache");
    }

    @Override
    public void start() {
        logger.info("InfinispanSource is started.............");
        try {
            CacheListener listener =new CacheListener();
            cacheManager = new DefaultCacheManager(configPath);
            //cacheManager.addListener(listener);
            cache = cacheManager.getCache(cacheName);
            cache.addListener(listener);
        } catch (IOException e) {
            e.printStackTrace();
        }

        super.start();

    }

    @Override
    public void stop() {
        logger.info("InfinispanSource is stopped.............");
        cache.stop();
        cacheManager.stop();
        super.stop();
    }

    @Listener
    public class CacheListener {

        @CacheEntryCreated
        public void dataAdded(CacheEntryCreatedEvent event) {
            System.out.println("1");
            if (event.isPre()) {
                logger.debug("Going to add new entry [{}] created in the cache", event.getKey());
            } else {
                logger.debug("Added new entry [{}] to the cache", event.getKey());
            }
        }

        @CacheEntryRemoved
        public void dataRemoved(CacheEntryRemovedEvent event) {
            if (event.isPre()) {
                logger.debug("Going to remove entry [{}] created in the cache", event.getKey());
            } else {
                logger.debug("Removed entry [{}] from the cache", event.getKey());
            }
        }

        @CacheStarted
        public void cacheStarted(CacheStartedEvent event) {
            logger.debug("Cache Started");
        }

        @CacheStopped
        public void cacheStopped(CacheStoppedEvent event) {
            logger.debug("Cache Stopped");
        }

    }
}
