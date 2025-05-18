package dev.jlipka;

import com.fasterxml.jackson.annotation.JsonProperty;

public record TestEntity(@JsonProperty("ID") Integer id, @JsonProperty("VALUE") Integer value) {
}
