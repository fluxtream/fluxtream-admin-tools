package org.fluxtream.admintools;

import java.io.IOException;
import java.sql.*;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * User: candide
 * Date: 30/09/14
 * Time: 14:35
 */
public class FixUpChannelMappings {

    private final String FLUXTREAM_CAPTURE_DEVICE_NICKNAME = "FluxtreamCapture";
    private final String RUNKEEPER_DEVICE_NICKNAME = "runkeeper";
    private final String FITBIT_DEVICE_NICKNAME = "Fitbit";
    private final String WITHINGS_DEVICE_NICKNAME = "Withings";
    private final String MYMEE_DEVICE_NICKNAME = "Mymee";
    private final String ZEO_DEVICE_NICKNAME = "Zeo";
    private final String BODYMEDIA_DEVICE_NICKNAME = "BodyMedia";
    private final String JAWBONE_UP_DEVICE_NICKNAME = "Jawbone_UP";

    // as gathered from staging 10/1/2014
    public final List<String> connectorDeviceNicknames = Arrays.asList(new String[]{FLUXTREAM_CAPTURE_DEVICE_NICKNAME, RUNKEEPER_DEVICE_NICKNAME,
            FITBIT_DEVICE_NICKNAME, WITHINGS_DEVICE_NICKNAME, MYMEE_DEVICE_NICKNAME, ZEO_DEVICE_NICKNAME,
            BODYMEDIA_DEVICE_NICKNAME, JAWBONE_UP_DEVICE_NICKNAME});
    public final Map<String,Integer> apiCodes = new HashMap<String,Integer>();

    public FixUpChannelMappings() {
        apiCodes.put(FLUXTREAM_CAPTURE_DEVICE_NICKNAME, 42);
        apiCodes.put(RUNKEEPER_DEVICE_NICKNAME, 35);
        apiCodes.put(FITBIT_DEVICE_NICKNAME, 7);
        apiCodes.put(WITHINGS_DEVICE_NICKNAME, 4);
        apiCodes.put(MYMEE_DEVICE_NICKNAME, 110);
        apiCodes.put(ZEO_DEVICE_NICKNAME, 3);
        apiCodes.put(BODYMEDIA_DEVICE_NICKNAME, 88);
        apiCodes.put(JAWBONE_UP_DEVICE_NICKNAME, 1999);
    }

    public void run() throws SQLException, IOException {
        final Connection connect = Main.getConnection();
        final Statement statement = connect.createStatement();
        final ResultSet eachGuest = statement.executeQuery("SELECT id, username FROM Guest ORDER BY id");
        DatastoreUtils datastoreUtils = new DatastoreUtils();
        final PreparedStatement hasMappingStmt = connect.prepareStatement("SELECT count(*) FROM ChannelMapping WHERE guestId=? AND deviceName=? AND channelName=?");
        final PreparedStatement addMappingStmt = connect.prepareStatement("INSERT INTO ChannelMapping (apiKeyId, deviceName, channelName, internalDeviceName, internalChannelName, channelType, guestId, timeType) VALUES (?,?,?,?,?,?,?,?)");

        while (eachGuest.next()) {
            Long guestId = eachGuest.getLong(1);
            String username = eachGuest.getString(2);
            final DatastoreUtils.ChannelInfoResponse channelInfoResponse = datastoreUtils.listSources(guestId);
            if (channelInfoResponse!=null) {
                for (Map.Entry<String, DatastoreUtils.ChannelSpecs> specsEntry : channelInfoResponse.channel_specs.entrySet()) {
                    final String fullyQualifiedChannelName = specsEntry.getKey();
                    String internalDeviceName = fullyQualifiedChannelName.split("\\.")[0];
                    String internalChannelName = fullyQualifiedChannelName.split("\\.")[1];
                    String deviceName = internalDeviceName;
                    if (!connectorDeviceNicknames.contains(internalDeviceName))
                        deviceName = "FluxtreamCapture";
//                    final DatastoreUtils.ChannelSpecs channelSpecs = specsEntry.getValue();
                    hasMappingStmt.setLong(1, guestId);
                    hasMappingStmt.setString(2, deviceName);
                    hasMappingStmt.setString(3, fullyQualifiedChannelName);
                    boolean isMapped = hasMappingStmt.getResultSet()!=null && hasMappingStmt.getResultSet().next();
                    if (!isMapped) {
                        Long apiKeyId = getApiKeyId(guestId, deviceName);
                        if (apiKeyId==null) {
                            System.out.println(fullyQualifiedChannelName + " data should be cleaned up for guest " + guestId + "!!!");
                            continue;
                        }
                        System.out.println("we should add a channel mapping here: " + internalDeviceName + "/" + internalChannelName);
                        addMappingStmt.setLong(1, apiKeyId);
                        if (!deviceName.equals(internalDeviceName)) {
                            // if deviceName!=internalDeviceName, the deviceName is "FluxtreamCapture"
                            addMappingStmt.setString(2, deviceName);
                        } else {
                            // otherwise use the device Nickname as in the datastore
                            addMappingStmt.setString(2, internalDeviceName);
                        }
                        addMappingStmt.setString(3, internalChannelName);
                        addMappingStmt.setString(4, internalDeviceName);
                        addMappingStmt.setString(5, internalChannelName);
                        addMappingStmt.setInt(6, 0);
                        addMappingStmt.setLong(7, guestId);
                        if (internalDeviceName.equalsIgnoreCase("Fitbit")||internalDeviceName.equalsIgnoreCase("Zeo"))
                            addMappingStmt.setInt(8, 1);
                        else
                            addMappingStmt.setInt(8, 0);
                        addMappingStmt.executeUpdate();
                    }
                }
            } else {
                System.out.println("Could not parse channel mappings for " + username);
            }
        }
    }

    public Long getApiKeyId(final long guestId, final String deviceName) throws SQLException {
        final Connection connect = Main.getConnection();
        final Statement statement = connect.createStatement();
        final ResultSet resultSet = statement.executeQuery("SELECT id FROM ApiKey WHERE api=" + apiCodes.get(deviceName) + " AND guestId=" + guestId);
        if (resultSet.next())
            return resultSet.getLong(1);
        // if the user doesn't have a FluxtreamCapture connector, create one here
        else if (deviceName.equalsIgnoreCase("FluxtreamCapture")){
            final Statement addFluxtreamCaptureApiKey = connect.createStatement();
            addFluxtreamCaptureApiKey.executeUpdate("INSERT INTO ApiKey (api, guestId, status, synching) VALUES (42, " + guestId + ", 0, 'N')");
            final ResultSet apiKeyIdResult = statement.executeQuery("SELECT id FROM ApiKey WHERE api=" + apiCodes.get(deviceName) + " AND guestId=" + guestId);
            if (apiKeyIdResult.next())
                return resultSet.getLong(1);
            else {
                System.out.println("ERROR: We could not create a new ApiKey for deviceName=" + deviceName + ", guestId=" + guestId);
            }
        }
        // this is stale data that should be cleaned up
        return null;
    }

}
