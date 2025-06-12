package org.example.model;

import jakarta.xml.bind.annotation.XmlElement;
import jakarta.xml.bind.annotation.XmlElementWrapper;
import jakarta.xml.bind.annotation.XmlRootElement;
import lombok.Data;

import java.util.List;

@XmlRootElement(name = "ReviewXml")
@Data
public class ReviewXml {
    private String itemId;
    private List<Review> reviews;

    @Data
    public static class Review {
        private int reviewRating;
        private String reviewComment;
    }
}