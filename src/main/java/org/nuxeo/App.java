/*
 * (C) Copyright 2013 Nuxeo SAS <http://nuxeo.com>
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as published
 * by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA
 * 02111-1307, USA.
 *
 * Author: bdelbosc@nuxeo.com
 *
 */
package org.nuxeo;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Timer;
import metrics_influxdb.HttpInfluxdbProtocol;
import metrics_influxdb.InfluxdbReporter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * Test jdbc connection and network latency
 *
 */
public class App {

    private static final Log log = LogFactory.getLog(App.class);

    private static final MetricRegistry metrics = new MetricRegistry();
    private static final Timer connectionTimer = metrics.timer("connection");
    private static final Timer executionTimer = metrics.timer("execution");
    private static final Timer fecthTimer = metrics.timer("fecthing");

    private static final String CONFIG_KEY = "config";

    private static final String DEFAULT_CONFIG_FILE = "jdbctester.properties";

    private static final String REPEAT_KEY = "repeat";

    private static final String DEFAULT_REPEAT = "100";

    public static void main(String[] args) throws SQLException, IOException {

        Properties prop = readProperties();
        String user = prop.getProperty("user");
        String password = prop.getProperty("password");
        String connectionURL = prop.getProperty("url");
        String driver = prop.getProperty("driver");
        String query = prop.getProperty("query");
        List<String> reporterList = Arrays.asList(prop.getProperty("reporter").trim().toLowerCase().split(","));

        log.info("Connect to:" + connectionURL + " from " + getHostName());
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream printStream = new PrintStream(baos);

        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        Timer.Context tc = null;
        int repeat = Integer.valueOf(
                System.getProperty(REPEAT_KEY, DEFAULT_REPEAT)).intValue();

        log.info("Submiting " + repeat + " queries: " + query);
        try {
            Class.forName(driver);
            tc = connectionTimer.time();
            conn = DriverManager.getConnection(connectionURL, user, password);
            tc.stop();
            ps = conn.prepareStatement(query,
                    ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_READ_ONLY);

            int paramCount = countOccurrences(query, '?');
            for (int i = 1; i <= paramCount; i++) {
                String key = "p" + i;
                String param = prop.getProperty(key);
                if (param == null) {
                    break;
                }
                log.info(key + " = " + param);
                String type = "object";
                if (param.contains(":")) {
                    type = param.split(":", 2)[0];
                    param = param.split(":", 2)[1];
                }
                if (type.equalsIgnoreCase("object")) {
                    ps.setObject(i, (Object) param);
                } else if (type.equalsIgnoreCase("string")) {
                    ps.setString(i, param);
                } else if (type.equalsIgnoreCase("nstring")) {
                    ps.setNString(i, param);
                } else {
                    log.warn("Unknown type " + type + " use setObject");
                    ps.setObject(i, (Object) param);
                }
            }

            int rows = 0;
            int bytes = 0;

            for (int i = 0; i < repeat; i++) {
                tc = executionTimer.time();
                rs = ps.executeQuery();
                tc.stop();
                tc = fecthTimer.time();
                ResultSetMetaData rsmd = rs.getMetaData();
                int cols = rsmd.getColumnCount();
                while (rs.next()) {
                    rows++;
                    for (int c = 1; c <= cols; c++) {
                        bytes += rs.getBytes(1).length;
                    }
                }
                rs.close();
                tc.stop();
                // don't stress too much
                Thread.sleep((int) (Math.random() * 100));
            }
            log.info("Fetched rows: " + rows + ", total bytes: " + bytes
                    + ", bytes/rows: " + ((float) bytes) / rows);

        } catch (SQLException e) {
            log.error(e.getMessage(), e);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        } finally {
            if (rs != null) {
                rs.close();
            }
            if (ps != null) {
                ps.close();
            }
            if (conn != null) {
                conn.close();
            }
        }
        consoleReporter(reporterList, baos);
        influxDbReporter(reporterList, prop);
    }

    private static void influxDbReporter(List<String> reporterList, Properties prop) {
        if (reporterList.contains("influxdb")) {
            final String influxDbHost = prop.getProperty("influxdb.host");
            final Integer influxDbPort = Integer.valueOf(prop.getProperty("influxdb.port"));
            final String influxDbUsername = prop.getProperty("influxdb.username");
            final String influxDbPassword = prop.getProperty("influxdb.password");
            final String influxDbName = prop.getProperty("influxdb.db");
            final ScheduledReporter influxdbReporter = InfluxdbReporter.forRegistry(metrics)
                    .protocol(new HttpInfluxdbProtocol(influxDbHost, influxDbPort, influxDbUsername, influxDbPassword, influxDbName))
                    .tag("server", getHostName())
                    .tag("application", "jdbcTester")
                    .build();
            influxdbReporter.report();
        }
    }

    private static void consoleReporter(List<String> reporterList, ByteArrayOutputStream baos) {
        if (reporterList.contains("console")) {
            final ConsoleReporter consoleReporter = ConsoleReporter.forRegistry(metrics)
                    .build();
            consoleReporter.report();
            try {
                String content = baos.toString("ISO-8859-1");
                log.info(content);
            } catch (UnsupportedEncodingException e) {
                log.error(e.getMessage(), e);
            }
        }
    }

    private static String getHostName() {
        String hostname;
        try {
            hostname = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            hostname = "unknown";
        }
        return hostname;
    }

    private static int countOccurrences(String haystack, char needle) {
        int count = 0;
        for (int i = 0; i < haystack.length(); i++) {
            if (haystack.charAt(i) == needle) {
                count++;
            }
        }
        return count;
    }

    private static Properties readProperties() throws IOException {
        Properties prop = new Properties();
        FileInputStream fs;
        try {
            fs = new FileInputStream(System.getProperty(CONFIG_KEY));
        } catch (FileNotFoundException e) {
            log.error(
                    "Property file not found: "
                            + System.getProperty(CONFIG_KEY, CONFIG_KEY), e);
            return null;
        }
        try {
            prop.load(fs);
            fs.close();
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        } catch (NullPointerException e) {
            log.error("File not found " + DEFAULT_CONFIG_FILE, e);
        }
        return prop;
    }
}
