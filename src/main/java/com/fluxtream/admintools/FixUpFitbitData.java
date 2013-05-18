package com.fluxtream.admintools;

import org.joda.time.DateTime;
import org.joda.time.DateTimeConstants;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.sql.*;

/**
 * User: candide
 * Date: 15/05/13
 * Time: 10:33
 */
class FixUpFitbitData {

    public static DateTimeFormatter dateStorageFormat = DateTimeFormat.forPattern(
            "yyyy-MM-dd");

    public void run() throws SQLException {
        LocalTimeStorageFixUpHelper.fixUpStartAndEndTimeUsingLocalTimeStorage("Facet_FitbitSleep");

        fixupFitbitWeight();
        fixupFitbitActivity();
    }

    private void fixupFitbitActivity() throws SQLException {
        final Connection connection = Main.getConnection();
        final Statement statement = connection.createStatement();
        final PreparedStatement pstmt = connection.prepareStatement("UPDATE Facet_FitbitActivity SET start=?, end=? WHERE id=?");
        final ResultSet resultSet = statement.executeQuery("SELECT id, date FROM Facet_FitbitActivity");
        while (resultSet.next()) {
            long id = resultSet.getLong(1);
            String dateStorage = resultSet.getString(2);
            final DateTime dateTime = dateStorageFormat.withZoneUTC().parseDateTime(dateStorage);

            long start = dateTime.getMillis();
            long end = dateTime.getMillis()+ DateTimeConstants.MILLIS_PER_DAY-1;

            pstmt.setLong(1, start);
            pstmt.setLong(2, end);
            pstmt.setLong(3, id);

            pstmt.executeUpdate();
        }
    }

    private void fixupFitbitWeight() throws SQLException {
        final Connection connection = Main.getConnection();
        final Statement statement = connection.createStatement();
        final PreparedStatement pstmt = connection.prepareStatement("UPDATE Facet_FitbitWeight SET start=?, end=? WHERE id=?");
        final ResultSet resultSet = statement.executeQuery("SELECT id, date FROM Facet_FitbitWeight");
        while (resultSet.next()) {
            long id = resultSet.getLong(1);
            String dateStorage = resultSet.getString(2);
            final DateTime dateTime = dateStorageFormat.withZoneUTC().parseDateTime(dateStorage);

            long start = dateTime.getMillis() + DateTimeConstants.MILLIS_PER_DAY/2;
            long end = dateTime.getMillis() + DateTimeConstants.MILLIS_PER_DAY/2;

            pstmt.setLong(1, start);
            pstmt.setLong(2, end);
            pstmt.setLong(3, id);

            pstmt.executeUpdate();
        }
    }

    public static void main(final String[] args) {
        final DateTime dateTime = dateStorageFormat.withZoneUTC().parseDateTime("2013-05-18");
        System.out.println(dateTime.getMillis());
    }


}
