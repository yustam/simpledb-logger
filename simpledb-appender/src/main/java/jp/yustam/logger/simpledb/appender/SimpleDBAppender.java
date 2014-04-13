package jp.yustam.logger.simpledb.appender;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.*;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.simpledb.AmazonSimpleDBClient;
import com.amazonaws.services.simpledb.model.*;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.log4j.Appender;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.spi.ErrorCode;
import org.apache.log4j.spi.LoggingEvent;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

public class SimpleDBAppender extends AppenderSkeleton implements Appender {

    private static final AWSCredentialsProviderChain CREDENTIALS_PROVIDER = new AWSCredentialsProviderChain(
            new EnvironmentVariableCredentialsProvider(),
            new SystemPropertiesCredentialsProvider(),
            new InstanceProfileCredentialsProvider(),
            new ClasspathPropertiesFileCredentialsProvider());

    /**
     * @see <a href="http://docs.aws.amazon.com/AmazonSimpleDB/latest/DeveloperGuide/SDBLimits.html">Limits - Amazon SimpleDB</a>
     */
    private static final int MAX_ITEMS_PAR_OPERATION = 25;

    private static final DateFormat SDF = new SimpleDateFormat("-yyyy-MM");

    private AmazonSimpleDBClient SDB_CLIENT;
    /**
     * AWS Region
     */
    protected String region = Regions.AP_NORTHEAST_1.getName();
    /**
     * SimpleDB Domain Name
     */
    protected String domainPrefix = "MyDomain";
    /**
     * Size of buffer
     */
    protected int bufferSize = 1;
    /**
     * Retry
     */
    protected int retry = 3;

    protected ArrayList<LoggingEvent> buffer;

    public SimpleDBAppender() {
        super();
        this.buffer = new ArrayList<LoggingEvent>(this.bufferSize);
    }

    public void append(LoggingEvent event) {
        this.buffer.add(event);
        if (this.buffer.size() >= this.bufferSize) {
            flushBuffer();
        }
    }

    protected void execute(List<ReplaceableItem> items) {
        int attempt = 0;
        while (attempt < this.retry) {
            String domainName = this.domainPrefix + SDF.format(new Date());
            if (SDB_CLIENT == null) {
                SDB_CLIENT = new AmazonSimpleDBClient(getCredentials());
                SDB_CLIENT.setRegion(Region.getRegion(Regions
                        .fromName(this.region)));
            }
            try {
                SDB_CLIENT.batchPutAttributes(new BatchPutAttributesRequest(
                        domainName, items));
                break;
            } catch (NoSuchDomainException ex) {
                SDB_CLIENT.setRegion(Region.getRegion(Regions
                        .fromName(this.region)));
                SDB_CLIENT.createDomain(new CreateDomainRequest(domainName));
            } catch (AmazonClientException ex) {
                errorHandler.error("AmazonClientException: ", ex,
                        ErrorCode.GENERIC_FAILURE);
                attempt++;
                SDB_CLIENT = null;
            }
        }
    }

    protected AWSCredentials getCredentials() {
        return CREDENTIALS_PROVIDER.getCredentials();
    }

    public void close() {
        flushBuffer();
        SDB_CLIENT.shutdown();
        this.closed = true;
    }

    public void flushBuffer() {
        List<ReplaceableItem> items = new ArrayList<ReplaceableItem>();
        for (Iterator<LoggingEvent> itr = this.buffer.iterator(); itr.hasNext(); ) {
            // Value exceeds maximum length of 1024
            LoggingEvent event = itr.next();
            List<ReplaceableAttribute> attrs = new ArrayList<ReplaceableAttribute>();
            attrs.add(new ReplaceableAttribute("LoggerName", event.getLoggerName(), false));
            if (event.getNDC() != null) {
                List<String> ndcs = Arrays.asList(event.getNDC().split(" "));
                for (int i = 0; i < ndcs.size(); i++) {
                    attrs.add(new ReplaceableAttribute("NDC_" + (i), ndcs.get(i), false));
                }
            }
            attrs.add(new ReplaceableAttribute("ThreadName", event.getThreadName(), false));
            attrs.add(new ReplaceableAttribute("Level", event.getLevel().toString(), false));
            attrs.add(new ReplaceableAttribute("LocationInformation", event.getLocationInformation().fullInfo, false));
            attrs.add(new ReplaceableAttribute("Message", event.getMessage().toString(), false));
            if (event.getThrowableInformation() != null) {
                Throwable ex = event.getThrowableInformation().getThrowable();
                attrs.add(new ReplaceableAttribute("ErrorClass", ex.getClass().getName(), false));
                attrs.add(new ReplaceableAttribute("ErrorMessage", StringUtils.defaultString(ex.getMessage()), false));
                int numError = 0;
                for (StackTraceElement trace : ex.getStackTrace()) {
                    attrs.add(new ReplaceableAttribute("ErrorTrace", (numError++) + "_" + trace.toString(), false));
                }
                Throwable rootCause = ExceptionUtils.getRootCause(ex);
                if (rootCause != null) {
                    attrs.add(new ReplaceableAttribute("CauseClass", rootCause.getClass().getName(), false));
                    attrs.add(new ReplaceableAttribute("CauseMessage", StringUtils.defaultString(rootCause.getMessage()), false));
                    int numCause = 0;
                    for (StackTraceElement trace : rootCause.getStackTrace()) {
                        attrs.add(new ReplaceableAttribute("CauseTrace", (numCause++) + "_" + trace.toString(), false));
                    }
                }
            }
            attrs.add(new ReplaceableAttribute("TimeStamp", Long.toString(event.timeStamp), false));
            items.add(new ReplaceableItem(RandomStringUtils.randomAlphanumeric(20), attrs));
        }
        execute(items);
        this.buffer.clear();
    }

    public void finalize() {
        close();
    }

    public boolean requiresLayout() {
        return false;
    }

    public String getRegion() {
        return this.region;
    }

    public void setRegion(String region) {
        this.region = region;
    }

    public String getDomainPrefix() {
        return this.domainPrefix;
    }

    public void setDomainPrefix(String domainPrefix) {
        this.domainPrefix = domainPrefix;
    }

    public void setBufferSize(int bufferSize) {
        if (bufferSize <= MAX_ITEMS_PAR_OPERATION) {
            this.bufferSize = bufferSize;
        } else {
            this.bufferSize = MAX_ITEMS_PAR_OPERATION;
        }
        this.buffer.ensureCapacity(this.bufferSize);
    }

    public int getBufferSize() {
        return this.bufferSize;
    }

    public void setRetry(int retry) {
        this.retry = retry;
    }

    public int getRetry() {
        return this.retry;
    }

}