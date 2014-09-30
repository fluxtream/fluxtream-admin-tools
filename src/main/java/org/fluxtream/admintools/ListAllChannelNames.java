package org.fluxtream.admintools;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * User: candide
 * Date: 30/09/14
 * Time: 18:00
 */
public class ListAllChannelNames {

    public void run() throws SQLException, IOException {
        final Connection connect = Main.getConnection();
        final Statement statement = connect.createStatement();
        final ResultSet eachGuest = statement.executeQuery("SELECT id, username FROM Guest ORDER BY id");
        DatastoreUtils datastoreUtils = new DatastoreUtils();

        Set<String> channelNames = new HashSet<String>();
        while (eachGuest.next()) {
            Long guestId = eachGuest.getLong(1);
            String username = eachGuest.getString(2);
            final DatastoreUtils.ChannelInfoResponse channelInfoResponse = datastoreUtils.listSources(guestId);
            if (channelInfoResponse!=null) {
                for (Map.Entry<String, DatastoreUtils.ChannelSpecs> specsEntry : channelInfoResponse.channel_specs.entrySet()) {
                    final String channelName = specsEntry.getKey();
                    final String deviceName = channelName.split(".")[0];
                    channelNames.add(channelName);
                }
            } else {
                System.out.println("Could not parse channel mappings for " + username);
            }
        }
        for (String channelName : channelNames) {
            System.out.println(channelName);
        }
    }

}
