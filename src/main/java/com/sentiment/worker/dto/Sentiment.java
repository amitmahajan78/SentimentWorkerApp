package com.sentiment.worker.dto;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
public class Sentiment {

    String sentiment;
    Float mixed;
    Float positive;
    Float neutral;
    Float negative;
    String location;

}
