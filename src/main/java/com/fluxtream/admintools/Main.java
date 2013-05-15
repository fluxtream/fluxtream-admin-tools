package com.fluxtream.admintools;

public class Main {

    public static void main(final String[] args) {
        if (args.length==0)
            menu();
        else {
            try {
                final int i = Integer.parseInt(args[0], 10);
                switch (i) {
                    case 1:
                        cleanupNullApiKeyIds();
                        break;
                    case 2:
                        fixUpZeoData();
                        break;
                }
            } catch (Exception e) {
                System.out.println("wrong argument: " + args[0]);
            }
        }
    }

    private static void fixUpZeoData() {
        System.out.println("fixing up zeo data...");
    }

    private static void cleanupNullApiKeyIds() {
        System.out.println("cleaning up null apiKeyIds...");
    }

    private static void menu() {
        System.out.println("please invoke this program with an integer argument as in:");
        System.out.println("\t  1: cleanup null apiKeyIds");
        System.out.println("\t  2: fix up zeo data");
    }

}
