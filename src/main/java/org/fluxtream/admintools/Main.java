package org.fluxtream.admintools;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class Main {

    static {
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public static Properties props;

    private static void loadProperties() throws IOException {
	String resPath = Main.class.getClassLoader().getResource("db.properties").getPath();
	System.out.println("Looking for db.properties in " + resPath);

        props = new Properties();
        InputStream inputStream = null;

	try {
	    inputStream = Main.class.getClassLoader()
                .getResourceAsStream("db.properties");
	}
	catch (Throwable e) {
	    inputStream = null;
	}

	if(inputStream==null) {
	    System.out.println("**** Could not find db.properties.  Please make sure it is in fluxtream-admin-tools/src/main/resources and rebuild");
	    System.exit(-1);
	}
	else {
	    props.load(inputStream);
	}
    }

    public static void main(final String[] args) throws Exception {
        if (args.length==0)
            menu();
        else {
            handleMenuChoice(args);
        }
    }

    private static void handleMenuChoice(String[] args) throws Exception {
        loadProperties();
        int i;
        try { i = Integer.parseInt(args[0], 10); }
        catch (NumberFormatException nfe) {
            throw nfe;
        }
        switch (i) {
            case 1:
                cleanupNullApiKeyIds();
                break;
            case 2:
                fixUpZeoData();
                break;
            case 3:
                fixUpFlickrData();
                break;
            case 4:
                fixUpFitbitData();
                break;
            case 5:
                storeApiKeyAttributes();
                break;
            case 6:
                listChannelNames();
                break;
            case 7:
                listDeviceNames();
                break;
            case 8:
                fixUpChannelMappings();
                break;
            case 911:
                exportMailingList();
                break;
            default:
                System.out.println("not a valid option");
        }
    }

    private static void listDeviceNames() throws IOException, SQLException {
        new ListAllDeviceNames().run();
    }

    private static void listChannelNames() throws IOException, SQLException {
        new ListAllChannelNames().run();
    }

    private static void fixUpChannelMappings() throws Exception {
        FixUpChannelMappings fixUpChannelMappings= new FixUpChannelMappings();
        fixUpChannelMappings.run();
    }

    private static void exportMailingList() throws IOException, SQLException {
        ExportMailingList exportMailingList = new ExportMailingList();
        exportMailingList.run();
    }

    private static void storeApiKeyAttributes() throws IOException, SQLException {
        StoreApiKeyAttributes storeApiKeyAttributes = new StoreApiKeyAttributes();
        storeApiKeyAttributes.run();
    }

    private static void fixUpFitbitData() throws Exception {
        FixUpFitbitData fixUpFitbitData = new FixUpFitbitData();
        fixUpFitbitData.run();
    }

    private static void fixUpFlickrData() throws Exception {
        FixUpFlickrData fixUpFlickrData = new FixUpFlickrData();
        fixUpFlickrData.run();
    }

    private static void fixUpZeoData() throws Exception {
        FixUpZeoData fixUpZeoData = new FixUpZeoData();
        fixUpZeoData.run();
    }

    private static void cleanupNullApiKeyIds() throws Exception {
        CleanupNullApiKeyIds cleanupNullApiKeyIds = new CleanupNullApiKeyIds();
        cleanupNullApiKeyIds.run();
    }

    private static void menu() {
        System.out.println("please invoke this program with an integer argument as in:");
        System.out.println("\t  1: cleanup null apiKeyIds");
        System.out.println("\t  2: fix up zeo data");
        System.out.println("\t  3: fix up flickr data");
        System.out.println("\t  4: fix up fitbit data");
        System.out.println("\t  5: store api key attributes");
        System.out.println("\t  6: list all channel names");
        System.out.println("\t  7: list all device names");
        System.out.println("\t  8: fix up channel mappings");
        System.out.println("\t  911: export mailing list");
    }

    public static Connection getConnection() throws SQLException {
        Connection connect = DriverManager
                .getConnection(Main.props.getProperty("db.url"));
        return connect;
    }

}
