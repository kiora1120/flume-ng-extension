import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by IntelliJ IDEA.
 * User: muda1120
 * Date: 11. 1. 19.
 * Time: PM 1:30
 * To change this template use File | Settings | File Templates.
 */
public class InfinispanSinkTest {

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
    }

    @Test
    public void getTest() {

        for (int i = 0; i < 100; i++) {
            logger.debug(cache.get(i).toString());
        }

    }
}

