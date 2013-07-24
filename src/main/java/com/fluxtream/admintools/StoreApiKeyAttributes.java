package com.fluxtream.admintools;

import org.apache.commons.lang.StringUtils;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.util.*;

/**
 * User: candide
 * Date: 01/07/13
 * Time: 13:05
 */
public class StoreApiKeyAttributes {

    private Properties props;
    private DesEncrypter encrypter;

    private void loadProperties() throws IOException {
        props = new Properties();
        InputStream inputStream = Main.class.getClassLoader()
                .getResourceAsStream("other.properties");
        props.load(inputStream);
        final String oauthPropertiesPath = (String) props.get("oauth.properties.path");
        inputStream = new FileInputStream(oauthPropertiesPath);
        props.load(inputStream);
        final String commonPropertiesPath = (String) props.get("common.properties.path");
        inputStream = new FileInputStream(commonPropertiesPath);
        props.load(inputStream);
    }

    public static void main(final String[] args) throws IOException, SQLException {
        StoreApiKeyAttributes p = new StoreApiKeyAttributes();
        p.run();
    }

    public void run() throws SQLException, IOException {
        loadProperties();
        encrypter = new DesEncrypter(props.getProperty("crypto"));

        System.out.println("trying to connect using url: " + Main.props.getProperty("db.url"));
        Connection connect = Main.getConnection();

        Statement statement;
        statement = connect.createStatement();

        List<Map<String,Object>> connectorInfos = getConnectorInfos(statement);

        for (Map<String, Object> connectorInfo : connectorInfos) {
            String[] attributeKeys = (String[]) connectorInfo.get("apiKeyAttributeKeys");

	    if(attributeKeys==null) {
		System.out.println("No attribute keys for " +  (String) connectorInfo.get("connectorName") + 
				   "; skipping");
		continue;
	    }

            // we first check that all the attributes are available
            for (String attributeKey : attributeKeys) {
                final Object attributeValue = props.get(attributeKey);
                if (attributeValue==null) {
                    System.out.println("missing attribute value for key " + attributeKey);
                    return;
                }
            }
            int api = (Integer) connectorInfo.get("api");
            String connectorName = (String) connectorInfo.get("connectorName");
            System.out.println("now processing " + connectorName);
            for (String attributeKey : attributeKeys) {
                final String attributeValue = props.getProperty(attributeKey);
                fillInApiKeyAttribute(api, attributeKey, attributeValue, connect);
            }
        }
    }

    private void fillInApiKeyAttribute(int api, String attributeKey, String attributeValue, Connection connect) throws SQLException {
        final Statement statement = connect.createStatement();
        final ResultSet eachApiKey = statement.executeQuery("SELECT id FROM ApiKey WHERE api=" + api);
        PreparedStatement pstmt = connect.prepareStatement("INSERT INTO ApiKeyAttribute (attributeKey, attributeValue, apiKey_id) VALUES (?,?,?)");
        final String encryptedValue = encrypter.encrypt(attributeValue);
        while(eachApiKey.next()) {
            long apiKeyId = eachApiKey.getLong(1);
            pstmt.setString(1, attributeKey);
            pstmt.setString(2, encryptedValue);
            pstmt.setLong(3, apiKeyId);
            System.out.println(String.format("apiKeyId:%s attributeKey:%s attributeValue:%s encryptedValue:%s", apiKeyId, attributeKey, attributeValue, encryptedValue));
            pstmt.executeUpdate();
        }
    }

    private List<Map<String, Object>> getConnectorInfos(Statement statement) throws SQLException {
        final ResultSet resultSet = statement.executeQuery("SELECT api, connectorName, apiKeyAttributeKeys FROM Connector");
        List<Map<String,Object>> infos = new ArrayList<Map<String,Object>>();
        while(resultSet.next()) {
            Map<String,Object> connectorInfo = new HashMap<String,Object>();
            final int api = resultSet.getInt("api");
            final String apiKeyAttributeKeys = resultSet.getString("apiKeyAttributeKeys");
            final String connectorName = resultSet.getString("connectorName");
            connectorInfo.put("api", api);
            connectorInfo.put("connectorName", connectorName);
            connectorInfo.put("apiKeyAttributeKeys", StringUtils.split(apiKeyAttributeKeys, ","));
            infos.add(connectorInfo);
        }
        return infos;
    }

}
