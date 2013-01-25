import junit.framework.Assert;
import org.infinispan.Cache;
import org.infinispan.manager.DefaultCacheManager;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Infinispan Local Client Test Case.
 * </p>
 *
 * @author <a href="mailto:jhshin9@gmail.com">Junghun, Shin (Korea)</a>
 * @version 1.0
 * @since 1.0
 */
public class InfinispanLocalClientTest {

    /**
     * SLF4J Logging
     */
    private Logger logger = LoggerFactory.getLogger(this.getClass());

    DefaultCacheManager cacheManager;

    Cache cache;

    @Before
    public void setUp() throws IOException {
        cacheManager = new DefaultCacheManager("config.xml");
        cache = cacheManager.getCache("sharedCache");
    }

    @Test
    public void putTest() {

        for (int i = 0; i < cache.size(); i++) {
            cache.put(i, i);
        }

        //   Assert.assertEquals(100, cache.size());
    }

    @Test
    public void getTest() {

        for (int i = 0; i < cache.size(); i++) {
            logger.debug(cache.get(i).toString());
        }

        Assert.assertEquals(100, cache.size());

    }

    @Test
    public void deleteTest() {
        cache.clear();
    }
}

