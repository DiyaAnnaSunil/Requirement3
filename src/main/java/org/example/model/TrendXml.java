package org.example.model;

import jakarta.xml.bind.annotation.XmlElement;
import jakarta.xml.bind.annotation.XmlRootElement;
import lombok.Data;

@XmlRootElement(name = "TrendXml")
@Data
public class TrendXml {
    private String itemId;
    private String categoryId;
    private String categoryName;
    private int availableStock;
    private int sellingPrice;
}