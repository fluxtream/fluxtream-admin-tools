package com.fluxtream.admintools;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * User: candide
 * Date: 15/05/13
 * Time: 10:33
 */
class FixUpFitbitData {

    public void run() throws SQLException {
        System.out.println("fixing up fitbit data");
        final Connection connection = Main.getConnection();
        final Statement statement = connection.createStatement();
        final ResultSet resultSet = statement.executeQuery("select count(*) from Facet_FitbitWeight");
        if (resultSet.next()) {
            System.out.println("number of weight facets: " + resultSet.getLong(1));
        }
    }

}
