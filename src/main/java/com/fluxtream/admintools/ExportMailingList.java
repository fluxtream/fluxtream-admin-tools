package com.fluxtream.admintools;

import org.joda.time.format.ISODateTimeFormat;

import java.io.Console;
import java.io.FileWriter;
import java.io.IOException;
import java.io.StringWriter;
import java.sql.*;

/**
 * User: candide
 * Date: 18/12/13
 * Time: 17:31
 */
public class ExportMailingList {

    public void run() throws SQLException, IOException {
        System.out.println("trying to connect using url: " + Main.props.getProperty("db.url"));

        Console console = System.console();
        String guestIdString = console.readLine("Since guest id (default 0): ");
        int guestId = 0;
        if (guestIdString!=null)
            try {
                guestId = Integer.valueOf(guestIdString);
            } catch (NumberFormatException e) {
                System.out.println("Please enter a number... exiting!");
            }

        Connection connect = Main.getConnection();
        final Statement statement = connect.createStatement();
        final ResultSet rs = statement.executeQuery("SELECT email, firstname, lastname, id, username, autoLoginTokenTimestamp FROM Guest WHERE id>" + guestId);
        StringWriter sw = new StringWriter();
        int count = 0;
        final ResultSetMetaData metaData = rs.getMetaData();
        while (rs.next()) {
            for (int i=1; i<=metaData.getColumnCount(); i++) {
                if (i>1) sw.write(", ");
                final int columnType = metaData.getColumnType(i);
                switch (columnType) {
                    case Types.BIGINT:
                        if (metaData.getColumnName(i).equalsIgnoreCase("autoLoginTokenTimestamp"))
                            sw.write(ISODateTimeFormat.dateHourMinuteSecond().withZoneUTC().print(rs.getLong(i)));
                        else
                            sw.write(String.valueOf(rs.getLong(i)));
                        break;
                    case Types.VARCHAR:
                    case Types.LONGVARCHAR:
                    case Types.NVARCHAR:
                    case Types.LONGNVARCHAR:
                        sw.write(rs.getString(i));
                        break;
                }
            }
            sw.write("\n");
            count++;
        }
        System.out.println(sw.toString());
        System.out.println(count + " guests added to mailing list");
        FileWriter fw = new FileWriter("mailing-list.csv");
        fw.write(sw.toString());
        fw.close();
    }

}
