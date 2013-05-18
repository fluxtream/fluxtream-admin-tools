package com.fluxtream.admintools;

import java.sql.*;
import java.util.*;

/**
 * User: candide
 * Date: 15/05/13
 * Time: 10:33
 */
class CleanupNullApiKeyIds {

    public static final String FACET_TABLE_NAMES = "Facet_BodymediaBurn " +
            "Facet_BodymediaSleep " +
            "Facet_BodymediaSteps " +
            "Facet_CalendarEventEntry " +
            "Facet_FitbitActivity " +
            "Facet_FitbitLoggedActivity " +
            "Facet_FitbitSleep " +
            "Facet_FitbitWeight " +
            "Facet_FlickrPhoto " +
            "Facet_FluxtreamCapturePhoto " +
            "Facet_GithubPush " +
            "Facet_LastFmLovedTrack " +
            "Facet_LastFmRecentTrack " +
            "Facet_Location " +
            "Facet_MymeeObservation " +
            "Facet_QuantifiedMindTest " +
            "Facet_RunKeeperFitnessActivity " +
            "Facet_Tweet " +
            "Facet_TwitterDirectMessage " +
            "Facet_TwitterMention " +
            "Facet_WithingsBPMMeasure " +
            "Facet_WithingsBodyScaleMeasure " +
            "Facet_ZeoSleepStats";

    public void run() throws SQLException {
        System.out.println("trying to connect using url: " + Main.props.getProperty("db.url"));

        Connection connect = Main.getConnection();

        ResultSet resultSet;
        Statement statement;
        statement = connect.createStatement();

        cleanupStaleData(connect);
        Map<String, Long> apiKeyIds = getApiKeyIds(statement);
        List<String> tableNames = getTableNames();

        for (String tableName : tableNames) {
            System.out.println("Processing " + tableName);

            resultSet = statement.executeQuery(String.format("select guestId, api from %s where apiKeyId is null group by guestId,api;", tableName));
            PreparedStatement pstmt = connect.prepareStatement(String.format("UPDATE %s SET apiKeyId=? WHERE api=? AND guestId=?", tableName));
            while (resultSet.next()) {
                final int api = resultSet.getInt("api");
                final long guestId = resultSet.getLong("guestId");
                Long apiKeyId = apiKeyIds.get(String.format("%s_%s", api, guestId));

                if (apiKeyId == null) {
                    System.out.println(String.format("  No apiKey for table %s api=%s, guestId=%s. To cleanup, execute:\n" +
                            "    DELETE from %s where api=%s and guestId=%s;",
                            tableName, api, guestId, tableName, api, guestId));
                    continue;
                }
                pstmt.setLong(1, apiKeyId);
                pstmt.setInt(2, api);
                pstmt.setLong(3, guestId);
                final int rowsUpdated = pstmt.executeUpdate();
                System.out.println("  " + rowsUpdated + " " + tableName + " rows have been assigned an apiKeyId");
            }
        }

        cleanupNullApiKeyIdsInUpdateWorkerTasks(connect, apiKeyIds);
    }

    private void cleanupNullApiKeyIdsInUpdateWorkerTasks(Connection connect, Map<String, Long> apiKeyIds) throws SQLException {
        Statement statement = connect.createStatement();
        ResultSet resultSet = statement.executeQuery("SELECT worker.guestId,conn.api, worker.connectorName from UpdateWorkerTask worker JOIN Connector conn ON worker.connectorName=conn.connectorName WHERE apiKeyId IS NULL GROUP BY guestId,api");
        PreparedStatement pstmt = connect.prepareStatement("UPDATE UpdateWorkerTask SET apiKeyId=? WHERE connectorName=? AND guestId=? AND apiKeyId IS NULL");
        while (resultSet.next()) {
            final long guestId = resultSet.getLong(1);
            final int api = resultSet.getInt(2);
            final String connectorName = resultSet.getString(3);
            Long apiKeyId = apiKeyIds.get(String.format("%s_%s", api, guestId));

            if (apiKeyId == null) {
                System.out.println(String.format("  No apiKey for UpdateWorkerTask connectorName=%s, guestId=%s. To cleanup, execute:\n" +
                        "    DELETE from UpdateWorkerTask where connectorName=%s and guestId=%s;",
                        connectorName, guestId, connectorName, guestId));
                continue;
            }
            pstmt.setLong(1, apiKeyId);
            pstmt.setString(2, connectorName);
            pstmt.setLong(3, guestId);
            final int rowsUpdated = pstmt.executeUpdate();
            System.out.println("  " + rowsUpdated + " " + connectorName + " UpdateWorkerTask rows have been assigned an apiKeyId");
        }
    }

    private Map<String, Long> getApiKeyIds(Statement statement) throws SQLException {
        ResultSet resultSet;
        resultSet = statement.executeQuery("SELECT * FROM ApiKey");
        Map<String, Long> apiKeys = new HashMap<String, Long>();
        while (resultSet.next()) {
            final int api = resultSet.getInt("api");
            final long guestId = resultSet.getLong("guestId");
            final long apiKeyId = resultSet.getLong("id");
            apiKeys.put(String.format("%s_%s", api, guestId), apiKeyId);
        }
        return apiKeys;
    }

    private List<String> getTableNames() {
        StringTokenizer st = new StringTokenizer(FACET_TABLE_NAMES);
        List<String> tableNames = new ArrayList<String>();
        while (st.hasMoreTokens())
            tableNames.add(st.nextToken());
        return tableNames;
    }

    public void cleanupStaleData(Connection connect) throws SQLException {
        long oneDayAgo = System.currentTimeMillis() - 86400000l;
        Statement statement = connect.createStatement();
        final int updateWorkerTasksDeleted = statement.executeUpdate(String.format("DELETE FROM UpdateWorkerTask WHERE not(status=2 AND updateType=2) and timeScheduled<%s", oneDayAgo));
        System.out.println("deleted " + updateWorkerTasksDeleted + " UpdateWorkerTasks");
        final int apiUpdatesDeleted = statement.executeUpdate(String.format("DELETE FROM ApiUpdates WHERE ts<%s", oneDayAgo));
        System.out.println("deleted " + apiUpdatesDeleted + " ApiUpdates");
    }

}
