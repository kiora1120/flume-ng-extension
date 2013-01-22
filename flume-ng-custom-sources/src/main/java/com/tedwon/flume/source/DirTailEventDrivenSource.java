package com.tedwon.flume.source;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileFilter;
import java.io.RandomAccessFile;
import java.util.Timer;
import java.util.TimerTask;

/**
 * Directory Tail Flume Source Class.
 * <p/>
 * EventDriven Style Implementation.
 * <p/>
 * Scan directory periodically
 * and detect the last modified file automatically.
 * <p/>
 * Parameters:<p/>
 * path<p/>
 * filePrefix<p/>
 * scanPeriod: default 1sec<p/>
 * debugThroughput: default false<p/>
 *
 * @author <a href="iamtedwon@gmail.com">Ted Won</a>
 * @version 1.0
 */
public class DirTailEventDrivenSource extends AbstractSource implements Configurable, EventDrivenSource {

    private static final Logger logger = LoggerFactory.getLogger(DirTailEventDrivenSource.class);

    private String path;
    private String filePrefix;
    private long scanPeriod;
    private boolean debugThroughput;

    private Timer scannerTimer;
    private Timer throughputTimer;

    private Thread tailThread;
    private boolean run;

    private File lastModifiedFile;

    private long totalCount;
    private long throughput;

    private ChannelProcessor channelProcessor;

    @Override
    public void configure(Context context) {
        String path = context.getString("path", "/tmp");
        String filePrefix = context.getString("filePrefix", "");
        long scanPeriod = context.getLong("scanPeriod", 1000L);
        boolean debugThroughput = context.getBoolean("debugThroughput", false);

        this.path = path;
        this.filePrefix = filePrefix;
        this.scanPeriod = scanPeriod;
        this.debugThroughput = debugThroughput;
    }

    @Override
    public void start() {

        logger.info("{} is starting..................", this.getClass().getSimpleName());

        channelProcessor = getChannelProcessor();

        scannerTimer = new Timer("scannerTimerThread", true);
        scannerTimer.scheduleAtFixedRate(new TimerTask() {

            public void run() {

                // new file
                File newLastModifiedFile = lastFileModified(path);

                // check for new file
                if (lastModifiedFile == null || !newLastModifiedFile.getPath().equals(lastModifiedFile.getPath())) {

                    // change
                    lastModifiedFile = newLastModifiedFile;

                    if (lastModifiedFile == null) return;

                    logger.info("Detected new last modified file: {}", lastModifiedFile.getPath());

                    run = false;
                    if (tailThread != null) {
                        tailThread.stop();
                    }
                    run = true;
                    TailRunner tailRunner = new TailRunner();
                    tailThread = new Thread(tailRunner);
                    tailThread.start();
                }
            }
        }, 0L, scanPeriod);

        if (debugThroughput) {
            throughputTimer = new Timer("throughputTimerThread", true);
            throughputTimer.scheduleAtFixedRate(new TimerTask() {

                long beforeTotalCount = 0;

                public void run() {
                    throughput = totalCount - beforeTotalCount;
                    logger.debug("totalCount= {}, throughput= {}", totalCount, throughput);

                    beforeTotalCount = totalCount;
                }
            }, 0L, 1000);
        }

        logger.info("{} is started successfully.", this.getClass().getSimpleName());
    }

    /**
     * Disconnect from external client and do any additional cleanup.
     */
    @Override
    public void stop() {

        channelProcessor.close();

        run = false;
        if (tailThread != null) {
            tailThread.stop();
        }

        if (scannerTimer != null) {
            try {
                scannerTimer.cancel();
            } catch (Exception e) {

            } finally {
                scannerTimer = null;
            }
        }

        if (throughputTimer != null) {
            try {
                throughputTimer.cancel();
            } catch (Exception e) {

            } finally {
                throughputTimer = null;
            }
        }
    }

    private class TailRunner implements Runnable {

        private RandomAccessFile randomFile;

        @Override
        public void run() {

            try {
                randomFile = new RandomAccessFile(lastModifiedFile, "r");
                randomFile.seek(randomFile.length());

                String line = null;
                while (run) {
                    line = randomFile.readLine();

                    if (line == null) {
                        Thread.sleep(10);
                        continue;
                    }

                    Event e = EventBuilder.withBody(line.getBytes());
                    channelProcessor.processEvent(e);

                    totalCount++;
                }
            } catch (Exception e) {
                logger.error("Error occurred: ", e);
            }
        }
    }

    /**
     * Find last modified file in the directory.
     *
     * @param dir directory
     * @return last modified file
     */
    private File lastFileModified(String dir) {
        File fl = new File(dir);
        File[] files = fl.listFiles(new FileFilter() {

            public boolean accept(File file) {
                if (file.getName().startsWith(filePrefix))
                    return true;

                return false;
            }
        });
        long lastMod = Long.MIN_VALUE;
        File choise = null;
        for (File file : files) {
            if (file.lastModified() > lastMod) {
                choise = file;
                lastMod = file.lastModified();
            }
        }
        return choise;
    }
}
