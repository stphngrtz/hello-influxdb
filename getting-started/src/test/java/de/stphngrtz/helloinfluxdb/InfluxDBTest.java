package de.stphngrtz.helloinfluxdb;

import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.*;
import org.junit.BeforeClass;
import org.junit.Test;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.TimeZone;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class InfluxDBTest {

    private static InfluxDB influxDB;

    @BeforeClass
    public static void setUp() throws Exception {
        influxDB = InfluxDBFactory.connect("http://192.168.99.100:8086", "root", "root");
    }

    @Test
    public void ping() throws Exception {
        Pong pong = influxDB.ping();
        assertFalse(Objects.equals(pong.getVersion(), "unknown"));
    }

    @Test
    public void create_describe_and_delete_database() throws Exception {
        String db = getClass().getSimpleName() + System.currentTimeMillis();
        influxDB.createDatabase(db);
        boolean dbFoundPostCreate = influxDB.describeDatabases().stream().anyMatch(d -> Objects.equals(d, db));
        influxDB.deleteDatabase(db);
        boolean dbFoundPostDelete = influxDB.describeDatabases().stream().anyMatch(d -> Objects.equals(d, db));

        assertTrue(dbFoundPostCreate);
        assertFalse(dbFoundPostDelete);
    }

    @Test
    public void writing_a_point() throws Exception {
        String db = getClass().getSimpleName() + System.currentTimeMillis();
        influxDB.createDatabase(db);

        influxDB.write(db, "default", Point
                .measurement("cpu")
                .tag("atag", "test")
                .field("idle", 90L)
                .field("usertime", 9L)
                .field("system", 1L)
                .build()
        );
        List<QueryResult.Result> results = influxDB.query(new Query("SELECT * FROM cpu GROUP BY *", db)).getResults();
        assertFalse(results.isEmpty());
        print(results);

        influxDB.deleteDatabase(db);
    }

    @Test
    public void writing_multiple_points() throws Exception {
        String db = getClass().getSimpleName() + System.currentTimeMillis();
        influxDB.createDatabase(db);

        influxDB.write(BatchPoints
                .database(db)
                .tag("async", "true")
                .retentionPolicy("default")
                .build()
                .point(Point
                        .measurement("cpu")
                        .tag("atag", "test")
                        .field("idle", 90L)
                        .field("usertime", 9L)
                        .field("system", 1L)
                        .build()
                )
                .point(Point
                        .measurement("disk")
                        .tag("atag", "test")
                        .field("used", 80L)
                        .field("free", 1L)
                        .build()
                )
        );

        List<QueryResult.Result> results = influxDB.query(new Query("SELECT * FROM cpu GROUP BY *", db)).getResults();
        assertFalse(results.isEmpty());
        print(results);

        influxDB.deleteDatabase(db);
    }

    @Test
    public void functions() throws Exception {
        String db = getClass().getSimpleName() + System.currentTimeMillis();
        influxDB.createDatabase(db);

        BatchPoints batchPoints = ZeitreihenFactory.create(db,
                new ZeitreihenFactory.ZeitreihenDTO("DE12345678900001", "strom", "rlm", format("01.01.2016 00:00"), format("31.12.2016 23:45"))
        );
        influxDB.write(batchPoints);

        List<QueryResult.Result> tag = influxDB.query(new Query("SELECT SUM(value), MAX(value) FROM lastgang WHERE time >= '2016-01-01T00:00:00Z' GROUP BY zaehlpunktbezeichnung, time(1d)", db)).getResults();
        List<QueryResult.Result> monat = influxDB.query(new Query("SELECT SUM(value), MAX(value) FROM lastgang WHERE time >= '2015-01-01T00:00:00Z' GROUP BY zaehlpunktbezeichnung, monat, jahr", db)).getResults();
        List<QueryResult.Result> jahr = influxDB.query(new Query("SELECT SUM(value), MAX(value) FROM lastgang WHERE time >= '2015-01-01T00:00:00Z' GROUP BY zaehlpunktbezeichnung, jahr", db)).getResults();

        // only 2376 values have been written. should be 96*366=35136. why is that?

        print(monat);

        influxDB.deleteDatabase(db);
    }

    private static long format(String text) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat("dd.MM.yyyy HH:mm");
        sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
        return sdf.parse(text).getTime();
    }

    private static void print(List<QueryResult.Result> results) {
        results.forEach(r -> (r.getSeries() == null ? Collections.<QueryResult.Series>emptySet() : r.getSeries()).forEach(s -> {
            System.out.println("name:" + s.getName());

            System.out.println("tags:");
            s.getTags().entrySet().forEach(e -> System.out.println("  " + e.getKey() + ":" + e.getValue()));

            System.out.println("values:");
            s.getValues().forEach(v -> {
                for (int i = 0; i < s.getColumns().size(); i++) {
                    System.out.println("  " + s.getColumns().get(i) + ":" + v.get(i));
                }
            });
        }));
    }
}
