package com.fluxtream.admintools;

import java.sql.SQLException;

/**
 * User: candide
 * Date: 15/05/13
 * Time: 10:33
 */
class FixUpFitbitData {

    public void run() throws SQLException {
        LocalTimeStorageFixUpHelper.fixUpStartAndEndTimeUsingLocalTimeStorage("Facet_FitbitSleep");

        fixupFitbitWeight();
        fixupFitbitActivity();
        fixupLoggedActivity();
    }

    private void fixupLoggedActivity() throws SQLException {
        LocalTimeStorageFixUpHelper.fixUpStartAndEndTimeUsingLocalTimeStorage("Facet_FitbitLoggedActivity");
    }

    private void fixupFitbitActivity() throws SQLException {
        LocalTimeStorageFixUpHelper.fixUpStartAndEndTimeUsingLocalTimeStorage("Facet_FitbitActivity");
    }

    private void fixupFitbitWeight() throws SQLException {
        LocalTimeStorageFixUpHelper.fixUpStartAndEndTimeUsingLocalTimeStorage("Facet_FitbitWeight");
    }


}
