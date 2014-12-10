package org.fluxtream.admintools;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * User: candideConnector
 * Date: 30/09/14
 * Time: 14:35
 */
public class FixUpChannelMappings {

    public FixUpChannelMappings() {
    }

    List<String> connectorDeviceNicknames = Arrays.asList("lastfm", "Flickr", "moves", "sms_backup", "google_calendar", "twitter", "Jawbone_UP", "Evernote", "lastfm",
            "BodyMedia", "Fitbit", "Mymee", "Withings", "runkeeper");

    public void run() throws Exception {
        final Connection connect = Main.getConnection();
        final Statement statement = connect.createStatement();
        final ResultSet eachGuest = statement.executeQuery("SELECT id, username FROM Guest ORDER BY id");
        DatastoreUtils datastoreUtils = new DatastoreUtils();
        final PreparedStatement hasMappingStmt = connect.prepareStatement("SELECT * FROM ChannelMapping WHERE guestId=? AND deviceName=? AND channelName=?");
        final PreparedStatement addMappingStmt = connect.prepareStatement("INSERT INTO ChannelMapping (apiKeyId, deviceName, channelName, internalDeviceName, internalChannelName, channelType, guestId, timeType, creationType) VALUES (?,?,?,?,?,?,?,?,1)");

        while (eachGuest.next()) {
            Long guestId = eachGuest.getLong(1);
            final DatastoreUtils.ChannelInfoResponse channelInfoResponse = datastoreUtils.listSources(guestId);
            if (channelInfoResponse!=null) {
                for (Map.Entry<String, DatastoreUtils.ChannelSpecs> specsEntry : channelInfoResponse.channel_specs.entrySet()) {
                    // a "fullyQualifiedChannelName" is the concatenation of a deviceName, a dot ('.') and a channelName
                    // some device names are "internal" (/free form) while others map cleanly to Fluxtream connectors.
                    // "Free form" device names will be assigned to the generic FluxtreamCapture connector here
                    final String fullyQualifiedChannelName = specsEntry.getKey();
                    String internalDeviceName = fullyQualifiedChannelName.split("\\.")[0];
                    String channelName = fullyQualifiedChannelName.split("\\.")[1];
                    String connectorNickname = internalDeviceName;
                    // the will only happen if we have previously run this fixup - we want to make it idempotent
                    if (!connectorDeviceNicknames.contains(internalDeviceName)) {
                        connectorNickname = "FluxtreamCapture";
                        hasMappingStmt.setLong(1, guestId);
                        hasMappingStmt.setString(2, connectorNickname);
                        hasMappingStmt.setString(3, channelName);
                        final ResultSet resultSet = hasMappingStmt.executeQuery();
                        boolean isMapped = resultSet != null && resultSet.next();
                        if (!isMapped) {
                            System.out.print("Couldn't find " + guestId + "/" + connectorNickname + "/" + channelName);
                            Long apiKeyId = getFluxtreamCaptureApiKeyId(guestId, connect);
                            if (apiKeyId == null) {
                                System.out.println("; couldn't find a corresponding apiKeyId either, thus skipping...");
                                continue;
                            }
                            addMappingStmt.setLong(1, apiKeyId);
                            addMappingStmt.setString(2, connectorNickname);
                            addMappingStmt.setString(3, channelName);
                            addMappingStmt.setString(4, internalDeviceName);
                            addMappingStmt.setString(5, channelName);
                            addMappingStmt.setInt(6, 0);
                            addMappingStmt.setLong(7, guestId);
                            if (internalDeviceName.equalsIgnoreCase("Fitbit") || internalDeviceName.equalsIgnoreCase("Zeo"))
                                addMappingStmt.setInt(8, 1);
                            else
                                addMappingStmt.setInt(8, 0);
                            System.out.println(" thus adding " + guestId + "/" + connectorNickname + "/" + channelName);
                            addMappingStmt.executeUpdate();
                        }
                    }
                }
            }
            // when we're done let's just delete all legacy mappings for this user
            connect.createStatement().execute("DELETE FROM ChannelMapping where guestId=" + guestId + " AND creationType=0");
        }
        // finally, update Gestalt and state that this fixup was successfully executed
        boolean hasGestalt = connect.createStatement().executeQuery("SELECT * FROM Gestalt").next();
        if (!hasGestalt)
            connect.createStatement().execute("INSERT INTO Gestalt (channelMappingsFixupWasExecuted) VALUES ('N')");
        connect.createStatement().execute("update Gestalt set channelMappingsFixupWasExecuted='Y'");
    }

    public Long getFluxtreamCaptureApiKeyId(final long guestId, Connection connect) throws Exception {
        final Statement statement = connect.createStatement();
        final ResultSet resultSet = statement.executeQuery("SELECT id FROM ApiKey WHERE api=42 AND guestId=" + guestId);
        if (resultSet.next())
            return resultSet.getLong(1);
        // if the user doesn't have a FluxtreamCapture connector, create one here
        final Statement addFluxtreamCaptureApiKey = connect.createStatement();
        addFluxtreamCaptureApiKey.executeUpdate("INSERT INTO ApiKey (api, guestId, status, synching) VALUES (42, " + guestId + ", 0, 'N')");
        final ResultSet apiKeyIdResult = statement.executeQuery("SELECT id FROM ApiKey WHERE api=42 AND guestId=" + guestId);
        if (apiKeyIdResult.next())
            return apiKeyIdResult.getLong(1);
        throw new Exception("Could not create FluxtreamCapture ApiKey for guest " + guestId);
    }

}
