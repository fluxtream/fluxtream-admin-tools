package com.fluxtream.admintools;

import java.io.IOException;
import java.sql.SQLException;

public class Main {

    public static void main(final String[] args) throws IOException, SQLException {
        if (args.length==0)
            menu();
        else {
            int i = -1;
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
                default:
                    System.out.println("not a valid option");
            }
        }
    }

    private static void fixUpZeoData() {
        System.out.println("fixing up zeo data...");
    }

    private static void cleanupNullApiKeyIds() throws SQLException, IOException {
        System.out.println("cleaning up null apiKeyIds...");
        CleanupNullApiKeyIds cleanupNullApiKeyIds = new CleanupNullApiKeyIds();
        cleanupNullApiKeyIds.run();
    }

    private static void menu() {
        System.out.println("please invoke this program with an integer argument as in:");
        System.out.println("\t  1: cleanup null apiKeyIds");
        System.out.println("\t  2: fix up zeo data");
    }

}
