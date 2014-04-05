package jp.yustam.logger.simpledb.sample;

import com.amazonaws.auth.*;
import com.amazonaws.regions.Region;
import com.amazonaws.services.simpledb.AmazonSimpleDBClient;
import com.amazonaws.services.simpledb.model.*;

import java.util.List;

public class Logging {

    private static final AWSCredentialsProviderChain CREDENTIALS_PROVIDER = new AWSCredentialsProviderChain(
            new EnvironmentVariableCredentialsProvider(),
            new SystemPropertiesCredentialsProvider(),
            new InstanceProfileCredentialsProvider(),
            new ClasspathPropertiesFileCredentialsProvider());

    private final Region region;

    public Logging(Region region) {
        this.region = region;
    }

    private AmazonSimpleDBClient getClient() {
        AWSCredentials credentials = CREDENTIALS_PROVIDER.getCredentials();
        AmazonSimpleDBClient client = new AmazonSimpleDBClient(credentials);
        client.setRegion(this.region);
        return client;
    }

    public List<Item> select(String sql) {
        AmazonSimpleDBClient client = getClient();
        SelectResult result = client.select(new SelectRequest().withSelectExpression(sql));
        return result.getItems();
    }

    public void createDomain(String domainName) {
        AmazonSimpleDBClient client = getClient();
        client.createDomain(new CreateDomainRequest(domainName));
    }

    public void deleteDomain(String domainName) {
        AmazonSimpleDBClient client = getClient();
        client.deleteDomain(new DeleteDomainRequest(domainName));
    }

}
