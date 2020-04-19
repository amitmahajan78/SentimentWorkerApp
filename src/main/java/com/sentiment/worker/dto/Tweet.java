package com.sentiment.worker.dto;

import lombok.*;

import java.time.LocalDateTime;


@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
@ToString
public class Tweet {

    String id;
    String timestamp;
    String tweet64;
    String location;
}

