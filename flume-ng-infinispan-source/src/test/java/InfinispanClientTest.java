import junit.framework.Assert;
import org.infinispan.Cache;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.manager.DefaultCacheManager;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

/**
 * Infinispan Client Test Case.
 * </p>
 *
 * @author <a href="mailto:jhshin9@gmail.com">Junghun, Shin (Korea)</a>
 * @version 1.0
 * @since 1.0
 */
public class InfinispanClientTest {

    /**
     * SLF4J Logging
     */
    private Logger logger = LoggerFactory.getLogger(this.getClass());

    RemoteCacheManager rcm;

    RemoteCache cache;

    @Before
    public void setUp() {
        rcm = new RemoteCacheManager("localhost:11222");
        cache = rcm.getCache("sharedCache");
    }

    @Test
    public void putTest() {

        for (int i = 0; i < 100; i++) {
            cache.put(i, i);
        }

        Assert.assertEquals(100, cache.size());
    }

    @Test
    public void getTest() throws IOException {
        DefaultCacheManager cm = new DefaultCacheManager("config.xml");
        Cache cache = cm.getCache("sharedCache");
        for (int i = 0; i < cache.size(); i++) {
            logger.debug(cache.get(i).toString());
        }

        Assert.assertEquals(100, cache.size());

    }

    @Test
    public void getCacheAll() {
        Map bulk = cache.getBulk();

        Iterator iterator = bulk.entrySet().iterator();
        while (iterator.hasNext()) {
            Object next = iterator.next();

            System.out.println(next.toString());

        }
    }

    @Test
    public void deleteTest() {
        cache.clear();
    }

    @Test
    public void test() {
        logger.debug("Cache Name : [{}]", cache.getName());
        logger.debug("Contains Key : [{}]", cache.containsKey(1));
        logger.debug("Cache Map : [{}]", cache);

        System.out.println(cache.getBulk());
    }
}

