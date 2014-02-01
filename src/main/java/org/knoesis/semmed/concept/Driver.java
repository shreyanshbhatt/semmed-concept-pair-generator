package org.knoesis.semmed.concept;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.XMLConfiguration;

public class Driver {

    private static final String DEFAULT_DB_DRIVER = "com.db.jdbc.Driver";

    private static final String KEY_DB_DRIVER = "db.driverclass";
    private static final String KEY_DB_DATABASE = "db.database";
    private static final String KEY_DB_USER = "db.user";
    private static final String KEY_DB_PASSWORD = "db.password";

    public static void main(String[] args) throws ConfigurationException {

        if (args.length < 1) {
            throw new ConfigurationException("Configuration XML not specified.");
        }

        Configuration config = new XMLConfiguration(args[0]);

        String dbDriver = config.getString(KEY_DB_DRIVER, DEFAULT_DB_DRIVER);
        String dbDatabase = config.getString(KEY_DB_DATABASE);
        String dbUser = config.getString(KEY_DB_USER);
        String dbPassword = config.getString(KEY_DB_PASSWORD);

    }

}
