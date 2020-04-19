package com.sentiment.worker;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.text.DecimalFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;

@SpringBootTest
class SenimentWorkerApplicationTests {

    @Test
    void contextLoads() {
    }

    @Test
    void testTwitterDateTimeFormat() {
        final String TWITTER = "EEE MMM dd HH:mm:ss ZZZ yyyy";
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(TWITTER);
        LocalDateTime dateTime = LocalDateTime.parse("Wed Apr 15 22:52:55 +0000 2020", formatter);
        ZonedDateTime zdt = ZonedDateTime.of(dateTime, ZoneId.systemDefault());
        System.out.println(zdt.toInstant().truncatedTo(ChronoUnit.HOURS).toEpochMilli());

        Instant instant = Instant.ofEpochMilli(zdt.toInstant().truncatedTo(ChronoUnit.HOURS).toEpochMilli());
        System.out.println(instant.atZone(ZoneId.systemDefault()).toLocalDateTime());


    }

    @Test
    void testNumberFormate() {
        DecimalFormat df = new DecimalFormat("#.###");
        String formatted = df.format(2002.456345);
        System.out.println(formatted);

    }

}
