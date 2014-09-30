package org.fluxtream.admintools;

import java.io.IOException;
import java.sql.*;
import java.util.Map;

/**
 * User: candide
 * Date: 30/09/14
 * Time: 14:35
 */
public class FixUpChannelMappings {

    public void run() throws SQLException, IOException {
        final Connection connect = Main.getConnection();
        final Statement statement = connect.createStatement();
        final ResultSet eachGuest = statement.executeQuery("SELECT id, username FROM Guest ORDER BY id");
        DatastoreUtils datastoreUtils = new DatastoreUtils();

        while (eachGuest.next()) {
            Long guestId = eachGuest.getLong(1);
            String username = eachGuest.getString(2);
            final DatastoreUtils.ChannelInfoResponse channelInfoResponse = datastoreUtils.listSources(guestId);
            for (Map.Entry<String,DatastoreUtils.ChannelSpecs> specsEntry : channelInfoResponse.channel_specs.entrySet()) {
                final String name = specsEntry.getKey();
                final DatastoreUtils.ChannelSpecs channelSpecs = specsEntry.getValue();
                System.out.println(name + ".channelType: " + channelSpecs.channelType);
                System.out.println(name + ".objectTypeName: " + channelSpecs.objectTypeName);
            }
        }
    }

}
