package org.example.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.bson.Document;

import java.io.Serializable;

public class StoreJson implements Serializable {
    @JsonProperty("_id")
    private String _id;

    private String itemName;
    private String categoryName;
    private Document itemPrice;
    private Document stockDetails;
    private boolean specialProduct;

    public String get_id() {
        return _id;
    }

    public void set_id(String _id) {
        this._id = _id;
    }

    public String getItemName() {
        return itemName;
    }

    public void setItemName(String itemName) {
        this.itemName = itemName;
    }

    public String getCategoryName() {
        return categoryName;
    }

    public void setCategoryName(String categoryName) {
        this.categoryName = categoryName;
    }

    public Document getItemPrice() {
        return itemPrice;
    }

    public void setItemPrice(Document itemPrice) {
        this.itemPrice = itemPrice;
    }

    public Document getStockDetails() {
        return stockDetails;
    }

    public void setStockDetails(Document stockDetails) {
        this.stockDetails = stockDetails;
    }

    public boolean isSpecialProduct() {
        return specialProduct;
    }

    public void setSpecialProduct(boolean specialProduct) {
        this.specialProduct = specialProduct;
    }
}