package jp.yustam.logger.simpledb.sample;

import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.simpledb.model.Attribute;
import com.amazonaws.services.simpledb.model.Item;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class LoggingTest {

    private static final Log LOG = LogFactory.getLog(LoggingTest.class);
    private static final DateFormat SDF = new SimpleDateFormat("-yyyy-MM");
    private static String DOMAIN_NAME = "MyLogs";

    @BeforeClass
    public static void setUpClass() {
        DOMAIN_NAME += SDF.format(new Date());
        Logging log = new Logging(Region.getRegion(Regions.AP_NORTHEAST_1));
        log.createDomain(DOMAIN_NAME);
    }

    @Test
    public void testInfo() {
        String expected = "This is my first Log Message.";
        LOG.info(expected);
        Logging log = new Logging(Region.getRegion(Regions.AP_NORTHEAST_1));
        String sql = String.format("select * from `%s` where Level = 'INFO'", DOMAIN_NAME);
        List<Item> result = log.select(sql);
        for (Item item : result) {
            for (Attribute attr : item.getAttributes()) {
                if (attr.getName().equals("Message")) {
                    assertThat(attr.getValue(), is(expected));
                }
            }
        }
    }

    @Test
    public void testWarn() {
        String expected = "java.lang.ArithmeticException";
        try {
            int num = 10 / 0;
        } catch (Exception ex) {
            LOG.warn("This is my warning log message.", ex);
        }
        Logging log = new Logging(Region.getRegion(Regions.AP_NORTHEAST_1));
        String sql = String.format("select * from `%s` where Level = 'WARN'", DOMAIN_NAME);
        List<Item> result = log.select(sql);
        for (Item item : result) {
            for (Attribute attr : item.getAttributes()) {
                if (attr.getName().equals("ErrorClass")) {
                    assertThat(attr.getValue(), is(expected));
                }
            }
        }
    }

    @AfterClass
    public static void tearDownClass() {
        Logging log = new Logging(Region.getRegion(Regions.AP_NORTHEAST_1));
        log.deleteDomain(DOMAIN_NAME);
    }

}
