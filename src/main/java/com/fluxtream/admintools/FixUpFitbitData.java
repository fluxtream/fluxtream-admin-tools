package com.fluxtream.admintools;

import java.sql.SQLException;

/**
 * User: candide
 * Date: 15/05/13
 * Time: 10:33
 */
class FixUpFitbitData {

    public void run() throws SQLException {
        LocalTimeStorageFixUpHelper.fixUpStartAndEndTimeUsingLocalTimeStorage("Facet_FitbitActivity");
        LocalTimeStorageFixUpHelper.fixUpStartAndEndTimeUsingLocalTimeStorage("Facet_FitbitWeight");
        LocalTimeStorageFixUpHelper.fixUpStartAndEndTimeUsingLocalTimeStorage("Facet_FitbitSleep");
        LocalTimeStorageFixUpHelper.fixUpStartAndEndTimeUsingLocalTimeStorage("Facet_FitbitLoggedActivity");
    }

}
