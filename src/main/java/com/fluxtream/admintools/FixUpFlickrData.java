package com.fluxtream.admintools;

import java.sql.SQLException;

/**
 * User: candide
 * Date: 15/05/13
 * Time: 10:33
 */
class FixUpFlickrData {

    public void run() throws SQLException {
        LocalTimeStorageFixUpHelper.fixUpStartAndEndTimeUsingLocalTimeStorage("Facet_FlickrPhoto");
    }

}
