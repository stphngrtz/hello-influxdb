package de.stphngrtz.helloinfluxdb;

import com.google.common.collect.Lists;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;

import java.math.BigDecimal;
import java.text.ParseException;
import java.util.*;
import java.util.concurrent.TimeUnit;

public final class ZeitreihenFactory {

    private ZeitreihenFactory() {
    }

    public static class ZeitreihenDTO {
        public final String zaehlpunktbezeichnung;
        public final String commodity;
        public final String zaehlverfahren;
        public final long von;
        public final long bis;

        public ZeitreihenDTO(String zaehlpunktbezeichnung, String commodity, String zaehlverfahren, long von, long bis) {
            this.zaehlpunktbezeichnung = zaehlpunktbezeichnung;
            this.commodity = commodity;
            this.zaehlverfahren = zaehlverfahren;
            this.von = von;
            this.bis = bis;
        }
    }

    public static Collection<BatchPoints> create(int chunksize, String db, ZeitreihenDTO... zeitreihenDTOs) throws ParseException {
        List<Point> points = new ArrayList<>();
        for (ZeitreihenDTO zeitreihenDTO : zeitreihenDTOs) {
            for (Long timestamp : timestamps(zeitreihenDTO)) {
                GregorianCalendar calendar = new GregorianCalendar();
                calendar.setTimeZone(TimeZone.getTimeZone("UTC"));
                calendar.setTimeInMillis(timestamp);
                int monat = calendar.get(Calendar.MONTH) + 1;
                int jahr = calendar.get(Calendar.YEAR);

                points.add(Point
                        .measurement("lastgang")
                        .tag("zaehlpunktbezeichnung", zeitreihenDTO.zaehlpunktbezeichnung)
                        .tag("commodity", zeitreihenDTO.commodity)
                        .tag("zaehlverfahren", zeitreihenDTO.zaehlverfahren)
                        .tag("monat", String.valueOf(monat))
                        .tag("jahr", String.valueOf(jahr))
                        .time(timestamp, TimeUnit.MILLISECONDS)
                        .field("value", randomValue())
                        .build()
                );
            }
        }

        Collection<BatchPoints> batchPoints = new ArrayList<>();
        Lists.partition(points, chunksize).forEach(chunk -> {
            BatchPoints bp = BatchPoints
                    .database(db)
                    .retentionPolicy("default")
                    .build();

            chunk.forEach(bp::point);
            batchPoints.add(bp);

        });
        return batchPoints;
    }

    public static BatchPoints create(String db, ZeitreihenDTO... zeitreihenDTOs) throws ParseException {
        BatchPoints batchPoints = BatchPoints
                .database(db)
                .retentionPolicy("default")
                .build();

        for (ZeitreihenDTO zeitreihenDTO : zeitreihenDTOs) {
            for (Long timestamp : timestamps(zeitreihenDTO)) {
                GregorianCalendar calendar = new GregorianCalendar();
                calendar.setTimeZone(TimeZone.getTimeZone("UTC"));
                calendar.setTimeInMillis(timestamp);
                int monat = calendar.get(Calendar.MONTH) + 1;
                int jahr = calendar.get(Calendar.YEAR);

                batchPoints.point(Point
                        .measurement("lastgang")
                        .tag("zaehlpunktbezeichnung", zeitreihenDTO.zaehlpunktbezeichnung)
                        .tag("commodity", zeitreihenDTO.commodity)
                        .tag("zaehlverfahren", zeitreihenDTO.zaehlverfahren)
                        .tag("monat", String.valueOf(monat))
                        .tag("jahr", String.valueOf(jahr))
                        .time(timestamp, TimeUnit.MILLISECONDS)
                        .field("value", randomValue())
                        .build()
                );
            }
        }
        return batchPoints;
    }

    private static List<Long> timestamps(ZeitreihenDTO zeitreihenDTO) {
        List<Long> timestamps = new ArrayList<>();
        for (long timestamp = zeitreihenDTO.von; timestamp <= zeitreihenDTO.bis; timestamp += 900000) { // 900000 = 15min
            timestamps.add(timestamp);
        }
        return timestamps;
    }

    private static BigDecimal randomValue() {
        Random random = new Random();
        return new BigDecimal(random.nextDouble() * 10);
    }
}
