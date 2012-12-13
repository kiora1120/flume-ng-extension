import org.apache.flume.*;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import twitter4j.*;
import twitter4j.auth.AccessToken;

import java.util.HashMap;
import java.util.Map;

/**
 * TwitterStreamSource
 *
 * @author <A HREF="mailto:[kiora1120@gmail.com]">TJune Kim</A>
 * @version 1.0
 */
public class TwitterStreamSource extends AbstractSource implements Configurable, EventDrivenSource {
    private static final Logger logger = LoggerFactory.getLogger(TwitterStreamSource.class);
    private TwitterAdapter adapter;

    private String[] track;
    private double[][] locations;
    private int count;
    private String consumerKey;
    private String consumerSecret;
    private String accessToken;
    private String accessTokenSecret;
//    private static String msg;



    @Override
    public void configure(Context context) {
        /*
                agent1.sources.source1.type = TweetSource
                agent1.sources.source1.track = 강예빈,아이유,북한,미사일,로켓,문재인,박근혜
                #agent1.sources.source1.locations = 11,11|22,22
                #agent1.sources.source1.count = 1000
                agent1.sources.source1.consumerKey=
                agent1.sources.source1.consumerSecret=
                agent1.sources.source1.accessToken=
                agent1.sources.source1.accessTokenSecret=

         */
        logger.info("Configure {}...", context.getString("track", ""));

        track = context.getString("track", "").split(",");
//        locations = makeLocationsDoubleArray(context.getString("locations", ""), "|");
        count = context.getInteger("count", -1);
        //todo 인증 키 받는 부분 넣어야 함.
        consumerKey = context.getString("consumerKey", "");
        consumerSecret = context.getString("consumerSecret", "");
        accessToken = context.getString("accessToken", "");
        accessTokenSecret = context.getString("accessTokenSecret", "");

    }

    public static double[][] makeLocationsDoubleArray(String locations, String splitKey) {
        String[] locationArray = locations.split(splitKey);
        int locationCount = locationArray.length;
        double[][] result = new double[locationCount][locationCount];


        return new double[0][];  //To change body of created methods use File | Settings | File Templates.
    }

    @Override
    public void start() {
        logger.info("Starting {}...", this);

        try {
            if (adapter == null) {
                adapter = new TwitterAdapter(getChannelProcessor(), track, count);
                adapter.setOAuth(consumerKey, consumerSecret, accessToken, accessTokenSecret);
            }
            adapter.run();
        } catch (InterruptedException e) {
            logger.error(e.getMessage(), e);
        }
        super.start();

        logger.info("Tweet source {} started.", getName());
    }

    @Override
    public void stop() {
        logger.info("Avro source {} stopping: {}", getName(), this);
        if (adapter != null) {
            adapter.shutdown();
            adapter = null;
        }
        super.stop();
    }


    public static class TwitterAdapter extends StatusAdapter {
        private ChannelProcessor channelProcessor;
        private String[] track;
        private int count;
        private TwitterStream twitterStream;

        public TwitterAdapter(ChannelProcessor channelProcessor, String[] track, int count) {
            this.channelProcessor = channelProcessor;
            this.track = track;
            this.count = count;
            twitterStream = new TwitterStreamFactory().getInstance();
        }

        public void run() throws InterruptedException {

            StatusListener listener = new StatusListener() {
                @Override
                public void onStatus(twitter4j.Status status) {
                    //todo Message 형태를 어떤식으로 가져가야 할 지 고민해야 함.
                    String msg = "@" + status.getUser().getScreenName() + " - " + status.getText();

                    if (msg != null || !msg.equals("")) {
                        Event e = EventBuilder.withBody(msg.getBytes());
                        Map<String,String> headerMap = new HashMap<String, String>();
                        headerMap.put("status","twitter");
                        e.setHeaders(headerMap);
                        channelProcessor.processEvent(e);
                    }
//
                }

                @Override
                public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
                    logger.info("Got a status deletion notice id:" + statusDeletionNotice.getStatusId());
                }

                @Override
                public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
                    logger.info("Got track limitation notice:" + numberOfLimitedStatuses);
                }

                @Override
                public void onScrubGeo(long userId, long upToStatusId) {
                    logger.info("Got scrub_geo event userId:" + userId + " upToStatusId:" + upToStatusId);
                }

                @Override
                public void onStallWarning(StallWarning warning) {
                    logger.info("Got stall warning:" + warning);
                }

                @Override
                public void onException(Exception e) {
                    logger.error(e.getMessage(), e);
                }
            };

            twitterStream.addListener(listener);
            FilterQuery query = new FilterQuery();
            if(count!=-1) query.count(count);
            query.track(track);
            twitterStream.filter(query);
        }

        public void shutdown() {
            if (twitterStream != null) twitterStream.shutdown();
        }

        public void setOAuth(String consumerKey, String consumerSecret, String accessToken, String accessTokenSecret) {
            twitterStream.setOAuthConsumer(consumerKey, consumerSecret);
            twitterStream.setOAuthAccessToken(new AccessToken(accessToken, accessTokenSecret));
        }
    }
}
