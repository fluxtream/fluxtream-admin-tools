package com.fluxtream.admintools;

import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.sql.*;

/**
 * User: candide
 * Date: 16/05/13
 * Time: 15:35
 */
public class LocalTimeStorageFixUpHelper {

    public static DateTimeFormatter timeStorageFormat = DateTimeFormat.forPattern(
            "yyyy-MM-dd'T'HH:mm:ss.SSS");

    public static void fixUpStartAndEndTimeUsingLocalTimeStorage(String tableName) throws SQLException {
        final Connection connection = Main.getConnection();
        final Statement statement = connection.createStatement();
        final PreparedStatement pstmt = connection.prepareStatement(String.format("UPDATE %s SET start=?, end=? WHERE id=?", tableName));
        final ResultSet resultSet = statement.executeQuery(String.format("SELECT id, startTimeStorage, endTimeStorage FROM %s WHERE startTimeStorage IS NOT NULL AND endTimeStorage IS NOT NULL", tableName));
        while (resultSet.next()) {
            long id = resultSet.getLong(1);
            String startTimeStorage = resultSet.getString(2);
            String endTimeStorage = resultSet.getString(3);

            long start = timeStorageFormat.withZoneUTC().parseMillis(startTimeStorage);
            long end = timeStorageFormat.withZoneUTC().parseMillis(endTimeStorage);

            pstmt.setLong(1, start);
            pstmt.setLong(2, end);
            pstmt.setLong(3, id);

            pstmt.executeUpdate();
        }
    }

}
