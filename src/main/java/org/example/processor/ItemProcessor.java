package org.example.processor;

import org.apache.camel.Exchange;
import org.bson.Document;
import org.example.model.ReviewXml;
import org.example.model.StoreJson;
import org.example.model.TrendXml;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

@Component
public class ItemProcessor {

    private static final Logger logger = LoggerFactory.getLogger(ItemProcessor.class);
    private static final SimpleDateFormat FORMATTER = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public void setCurrentTimestamp(Exchange exchange) {
        String currentTs;
        synchronized (FORMATTER) {
            currentTs = FORMATTER.format(new Date());
        }
        exchange.setProperty("currentTs", currentTs);
        logger.debug("Set currentTs: {}", currentTs);
    }

    public void prepareItemQuery(Exchange exchange) {
        Map<String, Date> controlRefMap = exchange.getProperty("controlRefMap", Map.class);
        Document query = new Document();

        if (controlRefMap != null && !controlRefMap.isEmpty()) {
            Date lastProcessTs = controlRefMap.get("global");
            if (lastProcessTs != null) {
                String lastProcessTsStr;
                synchronized (FORMATTER) {
                    lastProcessTsStr = FORMATTER.format(lastProcessTs);
                }
                query.append("lastUpdateDate", new Document("$gt", lastProcessTsStr));
                logger.debug("Prepared item query with lastUpdateDate > {}", lastProcessTsStr);
            } else {
                logger.warn("No lastProcessTs in controlRefMap, fetching all items");
            }
        } else {
            logger.warn("controlRefMap is null or empty, fetching all items");
        }

        exchange.getIn().setBody(query);
        logger.info("Prepared item query: {}", query.toJson());
    }

    @SuppressWarnings("unchecked")
    public void filterValidItems(Exchange exchange) {
        Object body = exchange.getIn().getBody();
        logger.debug("filterValidItems received body type: {}, value: {}",
                body != null ? body.getClass().getName() : "null", body);

        List<Document> items = body instanceof List ? (List<Document>) body : new ArrayList<>();
        Map<String, Date> controlRefMap = exchange.getProperty("controlRefMap", Map.class);
        List<Document> validItems = new ArrayList<>();

        if (items.isEmpty()) {
            logger.info("No items fetched from MongoDB, query: {}",
                    exchange.getIn().getBody(Document.class) != null ?
                            exchange.getIn().getBody(Document.class).toJson() : "null");
            exchange.getIn().setBody(validItems);
            return;
        }

        Date lastProcessTs = (controlRefMap != null && !controlRefMap.isEmpty()) ? controlRefMap.get("global") : null;

        for (Document item : items) {
            String id = item.getString("_id");
            String lastUpdateDateStr = item.getString("lastUpdateDate");

            if (lastUpdateDateStr == null) {
                logger.warn("Skipping item {}: lastUpdateDate is null", id);
                continue;
            }

            Date lastUpdateDate;
            try {
                synchronized (FORMATTER) {
                    lastUpdateDate = FORMATTER.parse(lastUpdateDateStr);
                }
            } catch (ParseException e) {
                logger.error("Invalid lastUpdateDate format for item {}: {}", id, lastUpdateDateStr, e);
                continue;
            }

            if (lastProcessTs == null || lastUpdateDate.after(lastProcessTs)) {
                validItems.add(item);
                logger.info("Valid item: {} with lastUpdateDate: {} (lastProcessTs: {})",
                        id, lastUpdateDateStr, lastProcessTs != null ? FORMATTER.format(lastProcessTs) : "none");
            } else {
                logger.debug("Skipping item {}: lastUpdateDate {} not after lastProcessTs {}",
                        id, lastUpdateDateStr, FORMATTER.format(lastProcessTs));
            }
        }

        logger.info("Fetched {} items, filtered to {} valid items: {}",
                items.size(), validItems.size(), validItems.stream().map(doc -> doc.getString("_id")).toList());
        exchange.getIn().setBody(validItems);
    }

    public void logFetchedItems(Exchange exchange) {
        List<Document> items = exchange.getIn().getBody(List.class);
        if (items != null && !items.isEmpty()) {
            List<String> itemSummaries = items.stream()
                    .map(doc -> doc.getString("_id") + "@" + doc.getString("lastUpdateDate"))
                    .toList();
            logger.info("Processing {} items: {}", items.size(), itemSummaries);
        } else {
            logger.info("No items to process after filtering");
        }
    }

    public void enrichWithCategory(Exchange exchange) {
        Document item = exchange.getIn().getBody(Document.class);
        if (item != null) {
            String categoryId = item.getString("categoryId");
            exchange.setProperty("itemId", item.getString("_id"));
            if (categoryId != null) {
                exchange.setProperty("item", item);
                Document query = new Document("_id", categoryId);
                exchange.getIn().setBody(query);
                logger.debug("Enriching item {} with categoryId {}, query: {}", item.getString("_id"), categoryId, query.toJson());
            } else {
                logger.warn("Item {} has no categoryId", item.getString("_id"));
                exchange.setProperty("item", item);
                exchange.setProperty("category", new Document("categoryName", "Unknown"));
            }
        } else {
            logger.warn("Item is null in enrichWithCategory");
            exchange.setProperty("category", new Document("categoryName", "Unknown"));
        }
    }

    public void processCategoryQuery(Exchange exchange) {
        Object body = exchange.getIn().getBody();
        String itemId = exchange.getProperty("itemId", String.class);
        String categoryId = exchange.getProperty("item", Document.class) != null ?
                exchange.getProperty("item", Document.class).getString("categoryId") : "unknown";
        logger.debug("Processing category query for item {} (categoryId: {}): body type={}, value={}",
                itemId, categoryId, body != null ? body.getClass().getName() : "null", body);

        Document resultDoc = null;
        if (body instanceof Document doc) {
            resultDoc = doc;
            logger.debug("Category query result for item {} (categoryId: {}): {}", itemId, categoryId, doc.toJson());
        } else {
            logger.warn("Category query for item {} (categoryId: {}): unexpected body type {}, value: {}, setting to default",
                    itemId, categoryId, body != null ? body.getClass().getName() : "null", body);
        }

        exchange.setProperty("category", resultDoc != null ? resultDoc : new Document("categoryName", "Unknown"));
        logger.debug("Set category property for item {} (categoryId: {}): {}",
                itemId, categoryId, exchange.getProperty("category", Document.class).toJson());
    }

    public void mapItemData(Exchange exchange) {
        Document itemDoc = exchange.getProperty("item", Document.class);
        Document categoryDoc = exchange.getProperty("category", Document.class);
        String categoryName = categoryDoc != null ? categoryDoc.getString("categoryName") : "Unknown";
        if (categoryDoc == null) {
            logger.warn("categoryDoc is null for item {}, using categoryName: Unknown", itemDoc != null ? itemDoc.getString("_id") : "unknown");
        }
        logger.debug("Mapping item {} with categoryDoc: {}, categoryName: {}",
                itemDoc != null ? itemDoc.getString("_id") : "unknown", categoryDoc != null ? categoryDoc.toJson() : "null", categoryName);

        if (itemDoc == null) {
            logger.error("itemDoc is null, cannot map item data");
            return;
        }

        TrendXml trendXml = new TrendXml();
        trendXml.setItemId(itemDoc.getString("_id"));
        trendXml.setCategoryId(itemDoc.getString("categoryId"));
        trendXml.setCategoryName(categoryName);
        Document stock = itemDoc.get("stockDetails", Document.class);
        int availableStock = 0;
        if (stock != null && stock.get("availableStock") != null) {
            Object stockValue = stock.get("availableStock");
            if (stockValue instanceof Number number) {
                availableStock = number.intValue();
            } else {
                logger.warn("Invalid availableStock type for item {}: {}", itemDoc.getString("_id"), stockValue);
            }
        }
        trendXml.setAvailableStock(availableStock);

        Document price = itemDoc.get("itemPrice", Document.class);
        int sellingPrice = 0;
        if (price != null && price.get("sellingPrice") != null) {
            Object priceValue = price.get("sellingPrice");
            logger.debug("Item {} itemPrice: {}, sellingPrice value: {}", itemDoc.getString("_id"), price.toJson(), priceValue);
            if (priceValue instanceof Number number) {
                sellingPrice = number.intValue();
                logger.debug("Set sellingPrice for item {} to {}", itemDoc.getString("_id"), sellingPrice);
            } else {
                logger.warn("Invalid sellingPrice type for item {}: {}", itemDoc.getString("_id"), priceValue);
            }
        } else {
            logger.warn("itemPrice or sellingPrice is null for item {}, itemPrice: {}", itemDoc.getString("_id"), price != null ? price.toJson() : "null");
        }
        trendXml.setSellingPrice(sellingPrice);

        ReviewXml reviewXml = new ReviewXml();
        reviewXml.setItemId(itemDoc.getString("_id"));
        List<ReviewXml.Review> reviews = new ArrayList<>();
        List<Document> reviewDocs = itemDoc.getList("review", Document.class, Collections.emptyList());
        for (Document r : reviewDocs) {
            ReviewXml.Review review = new ReviewXml.Review();
            Object ratingValue = r.get("rating");
            int rating = 0;
            if (ratingValue instanceof Number number) {
                rating = number.intValue();
            } else if (ratingValue instanceof String string) {
                try {
                    rating = Integer.parseInt(string);
                } catch (NumberFormatException e) {
                    logger.warn("Invalid rating string for item {}: {}", itemDoc.getString("_id"), ratingValue);
                }
            } else {
                logger.warn("Invalid rating type for item {}: {}", itemDoc.getString("_id"), ratingValue);
            }
            review.setReviewRating(rating);
            review.setReviewComment(r.getString("comment"));
            reviews.add(review);
        }
        reviewXml.setReviews(reviews);

        StoreJson storeJson = new StoreJson();
        storeJson.set_id(itemDoc.getString("_id"));
        storeJson.setItemName(itemDoc.getString("itemName"));
        storeJson.setCategoryName(categoryName);
        Document itemPriceDoc = itemDoc.get("itemPrice", Document.class);
        storeJson.setItemPrice(itemPriceDoc);
        logger.debug("Set itemPrice for StoreJson item {}: {}", itemDoc.getString("_id"), itemPriceDoc != null ? itemPriceDoc.toJson() : "null");
        storeJson.setStockDetails(itemDoc.get("stockDetails", Document.class));
        storeJson.setSpecialProduct(itemDoc.getBoolean("specialProduct", false));

        exchange.setProperty("trendXml", trendXml);
        exchange.setProperty("reviewXml", reviewXml);
        exchange.setProperty("storeJson", storeJson);
        logger.debug("Mapped item {} to TrendXml, ReviewXml, StoreJson", itemDoc.getString("_id"));
    }

    public void prepareTrendXml(Exchange exchange) {
        TrendXml trendXml = exchange.getProperty("trendXml", TrendXml.class);
        String itemId = exchange.getProperty("itemId", String.class);
        if (trendXml == null || trendXml.getItemId() == null || itemId == null) {
            logger.warn("trendXml or itemId is null or invalid, skipping");
            exchange.getIn().setBody(null);
            return;
        }
        exchange.getIn().setHeader("CamelFileName", String.format("trend-%s.xml", itemId));
        exchange.getIn().setBody(trendXml);
        logger.debug("Prepared trend XML for item: {}", trendXml.getItemId());
    }

    public void prepareStoreJson(Exchange exchange) {
        StoreJson storeJson = exchange.getProperty("storeJson", StoreJson.class);
        String itemId = exchange.getProperty("itemId", String.class);
        if (storeJson == null || storeJson.get_id() == null || itemId == null) {
            logger.warn("storeJson or itemId is null or invalid, skipping");
            exchange.getIn().setBody(null);
            return;
        }
        exchange.getIn().setHeader("CamelFileName", String.format("storefront-%s.json", itemId));
        exchange.getIn().setBody(storeJson);
        logger.debug("Prepared store JSON for item: {}", storeJson.get_id());
    }

    public void prepareReviewXml(Exchange exchange) {
        ReviewXml reviewXml = exchange.getProperty("reviewXml", ReviewXml.class);
        String itemId = exchange.getProperty("itemId", String.class);
        if (reviewXml == null || reviewXml.getItemId() == null || itemId == null) {
            logger.warn("reviewXml or itemId is null or invalid, skipping");
            exchange.getIn().setBody(null);
            return;
        }
        exchange.getIn().setHeader("CamelFileName", String.format("review-%s.xml", itemId));
        exchange.getIn().setBody(reviewXml);
        logger.debug("Prepared review XML for item: {}", reviewXml.getItemId());
    }
}