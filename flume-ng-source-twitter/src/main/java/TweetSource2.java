import org.apache.flume.*;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.*;

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
public class TweetSource2 extends AbstractSource implements Configurable, EventDrivenSource {
    private static final Logger logger = LoggerFactory.getLogger(TweetSource2.class);
    private String[] track;
    private static String msg = "";
    private TwitterAdapter adapter;


    @Override
    public void configure(Context context) {
        logger.info("Configure {}...", context.getString("track",""));
        //todo 인증 키 받는 부분 넣어야 함.

        track = context.getString("track").split(",");
//    some_Param = context.get("some_param", String.class);
        // process some_param …


    }

    @Override
    public void start() {
        logger.info("Starting {}...", this);

        try {
            if (adapter == null) adapter = new TwitterAdapter(getChannelProcessor(),track);
            adapter.run();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        super.start();

        logger.info("Tweet source {} started.", getName());
    }

    @Override
    public void stop() {
        logger.info("Avro source {} stopping: {}", getName(), this);
        if (adapter != null){
            adapter.shutdown();
            adapter = null;
        }
        super.stop();
    }


    public static class TwitterAdapter extends StatusAdapter {
        private ChannelProcessor channelProcessor;
        private String[] track;
        private TwitterStream twitterStream;

        public TwitterAdapter(ChannelProcessor channelProcessor, String[] track) {
            this.channelProcessor = channelProcessor;
            this.track = track;
        }

        public void run() throws InterruptedException {
            twitterStream = new TwitterStreamFactory().getInstance();

            StatusListener listener = new StatusListener() {
                @Override
                public void onStatus(twitter4j.Status status) {

                    msg = "@" + status.getUser().getScreenName() + " - " + status.getText();

                    if (msg != null || !msg.equals("")) {
                        Event e = EventBuilder.withBody(msg.getBytes());
                        channelProcessor.processEvent(e);
                    }
//
                }

                @Override
                public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
                    System.out.println("Got a status deletion notice id:" + statusDeletionNotice.getStatusId());
                }

                @Override
                public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
                    System.out.println("Got track limitation notice:" + numberOfLimitedStatuses);
                }

                @Override
                public void onScrubGeo(long userId, long upToStatusId) {
                    System.out.println("Got scrub_geo event userId:" + userId + " upToStatusId:" + upToStatusId);
                }

                @Override
                public void onStallWarning(StallWarning warning) {
                    System.out.println("Got stall warning:" + warning);
                }

                @Override
                public void onException(Exception ex) {
                    ex.printStackTrace();
                }
            };
            twitterStream.addListener(listener);
            FilterQuery query = new FilterQuery();
            query.track(track);
            twitterStream.filter(query);
        }

        public void shutdown(){
            if(twitterStream!=null) twitterStream.shutdown();
        }
    }
}
