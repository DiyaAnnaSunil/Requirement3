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
        logger.debug("Fetching controlRef document from MongoDB");
        if (mongoTemplate == null) {
            logger.error("MongoTemplate is not injected");
            exchange.setProperty("controlRefMap", new HashMap<String, Date>());
            return;
        }

        try {
            // Fetch single controlRef document with _id: "global"
            Document controlRefDoc = mongoTemplate.getCollection("controlRef").find(new Document("_id", "global")).first();
            Map<String, Date> controlRefMap = new HashMap<>();

            if (controlRefDoc != null) {
                logger.debug("Found controlRef document: {}", controlRefDoc.toJson());
                String lastProcessTsStr = controlRefDoc.getString("lastProcessTs");
                if (lastProcessTsStr != null) {
                    try {
                        Date lastProcessTs;
                        synchronized (FORMATTER) {
                            lastProcessTs = FORMATTER.parse(lastProcessTsStr);
                        }
                        controlRefMap.put("global", lastProcessTs);
                        logger.debug("Added controlRef entry: lastProcessTs={}", lastProcessTsStr);
                    } catch (Exception e) {
                        logger.warn("Invalid lastProcessTs format: {}", lastProcessTsStr, e);
                    }
                } else {
                    logger.warn("Invalid controlRef document, missing lastProcessTs: {}", controlRefDoc.toJson());
                }
            } else {
                logger.info("No controlRef document found for _id: 'global', using empty map");
            }

            exchange.setProperty("controlRefMap", controlRefMap);
            logger.info("Fetched controlRefMap with {} entries", controlRefMap.size());
        } catch (Exception e) {
            logger.error("Failed to fetch controlRef document", e);
            exchange.setProperty("controlRefMap", new HashMap<String, Date>());
        }
    }

    public void updateControlRef(Exchange exchange) {
        String currentTs = exchange.getProperty("currentTs", String.class);

        if (currentTs == null) {
            logger.warn("Cannot update controlRef: currentTs is null");
            exchange.getIn().setBody(null);
            return;
        }

        try {
            Document controlRefDoc = new Document("_id", "global")
                    .append("lastProcessTs", currentTs);
            exchange.getIn().setBody(controlRefDoc);
            logger.info("Prepared controlRef update: lastProcessTs={}", currentTs);
        } catch (Exception e) {
            logger.error("Failed to prepare controlRef update", e);
            exchange.getIn().setBody(null);
        }
    }
}
