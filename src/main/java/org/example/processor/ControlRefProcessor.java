package org.example.processor;

import org.apache.camel.Exchange;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Component;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

@Component("controlRefProcessor")
public class ControlRefProcessor {

    private static final Logger logger = LoggerFactory.getLogger(ControlRefProcessor.class);
    private static final SimpleDateFormat FORMATTER = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    @Autowired
    private MongoTemplate mongoTemplate;

    public void fetchControlRefs(Exchange exchange) {
        logger.debug("Fetching controlRef documents from MongoDB");
        if (mongoTemplate == null) {
            logger.error("MongoTemplate is not injected");
            exchange.setProperty("controlRefMap", new HashMap<String, Date>());
            return;
        }

        try {
            Iterable<Document> controlRefDocs = mongoTemplate.getCollection("controlRef").find();
            Map<String, Date> controlRefMap = new HashMap<>();
            int count = 0;

            for (Document doc : controlRefDocs) {
                logger.debug("Processing controlRef document: {}", doc.toJson());
                String itemId = doc.getString("_id");
                String lastProcessTsStr = doc.getString("lastProcessTs");
                if (itemId != null && lastProcessTsStr != null) {
                    try {
                        Date lastProcessTs;
                        synchronized (FORMATTER) {
                            lastProcessTs = FORMATTER.parse(lastProcessTsStr);
                        }
                        controlRefMap.put(itemId, lastProcessTs);
                        logger.debug("Added controlRef entry: itemId={}, lastProcessTs={}", itemId, lastProcessTsStr);
                        count++;
                    } catch (Exception e) {
                        logger.warn("Invalid lastProcessTs format for item {}: {}", itemId, lastProcessTsStr, e);
                    }
                } else {
                    logger.warn("Invalid controlRef document: {}", doc.toJson());
                }
            }

            exchange.setProperty("controlRefMap", controlRefMap);
            logger.info("Fetched {} controlRef documents", count);
        } catch (Exception e) {
            logger.error("Failed to fetch controlRef documents", e);
            exchange.setProperty("controlRefMap", new HashMap<String, Date>());
        }
    }

    public void updateControlRef(Exchange exchange) {
        String itemId = exchange.getProperty("itemId", String.class);
        String currentTs = exchange.getProperty("currentTs", String.class);

        if (itemId == null || currentTs == null) {
            logger.warn("Cannot update controlRef: itemId or currentTs is null");
            return;
        }

        try {
            Document controlRefDoc = new Document("_id", itemId)
                    .append("lastProcessTs", currentTs);
            exchange.getIn().setBody(controlRefDoc);
            logger.info("Prepared controlRef update for itemId: {}, lastProcessTs: {}", itemId, currentTs);
        } catch (Exception e) {
            logger.error("Failed to prepare controlRef update for itemId: {}", itemId, e);
        }
    }
}