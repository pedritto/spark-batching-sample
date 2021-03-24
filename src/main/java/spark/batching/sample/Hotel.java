package spark.batching.sample;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class Hotel {
    private String id;
    private String name;
    private String country;
    private String city;
    private String address;
    private Double latitude;
    private Double longitude;
}
