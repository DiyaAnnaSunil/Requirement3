package org.example.route;

import org.apache.camel.LoggingLevel;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.jackson.JacksonDataFormat;
import org.apache.camel.component.mongodb.MongoDbConstants;
import org.apache.camel.converter.jaxb.JaxbDataFormat;
import org.example.model.ReviewXml;
import org.example.model.StoreJson;
import org.example.model.TrendXml;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class ItemRoute extends RouteBuilder {
    private static final Logger logger = LoggerFactory.getLogger(ItemRoute.class);

    @Override
    public void configure() throws Exception {
        JaxbDataFormat trendXmlFormat = new JaxbDataFormat(TrendXml.class.getPackage().getName());
        JaxbDataFormat reviewXmlFormat = new JaxbDataFormat(ReviewXml.class.getPackage().getName());
        JacksonDataFormat jsonFormat = new JacksonDataFormat(StoreJson.class);

        String mongoUri = "mongodb:mongoClient?database={{app.mongodb.database}}";

        from("quartz://fileExport?cron={{app.scheduler.cron}}")
                .routeId("fileExport")
                .bean("itemProcessor", "setCurrentTimestamp")
                .to("direct:fetchControlRef")
                .to("direct:processItems")
                .to("direct:updateControlRef")
                .log(LoggingLevel.INFO, "File export completed");

        from("direct:fetchControlRef")
                .routeId("fetchControlRef")
                .bean("controlRefProcessor", "fetchControlRefs")
                .log(LoggingLevel.INFO, "Fetched controlRefMap with ${exchangeProperty.controlRefMap.size()} entries");

        from("direct:processItems")
                .routeId("processItems")
                .bean("itemProcessor", "prepareItemQuery")
                .setHeader(MongoDbConstants.LIMIT, constant(Integer.parseInt(getContext().resolvePropertyPlaceholders("{{app.records.processLimit}}"))))
                .to(mongoUri + "&collection={{app.item.collection}}&operation=findAll")
                .process(exchange -> {
                    Object body = exchange.getIn().getBody();
                    logger.debug("Post-findAll body type: {}, value: {}",
                            body != null ? body.getClass().getName() : "null", body);
                    if (!(body instanceof java.util.List)) {
                        logger.warn("Unexpected findAll result type: {}, converting to empty list",
                                body != null ? body.getClass().getName() : "null");
                        exchange.getIn().setBody(new java.util.ArrayList<>());
                    }
                })
                .bean("itemProcessor", "filterValidItems")
                .bean("itemProcessor", "logFetchedItems")
                .split(body()).parallelProcessing()
                .bean("itemProcessor", "enrichWithCategory")
                .log(LoggingLevel.DEBUG, "Executing category query for item ${exchangeProperty.itemId} on ${header.CamelMongoDbDatabase}.${header.CamelMongoDbCollection}")
                .to(mongoUri + "&collection={{app.category.collection}}&operation=findOneByQuery&outputType=Document")
                .process(exchange -> {
                    Object body = exchange.getIn().getBody();
                    logger.debug("Post-findOneByQuery body type: {}, value: {}",
                            body != null ? body.getClass().getName() : "null", body);
                    if (body instanceof java.util.List) {
                        logger.warn("Unexpected findOneByQuery result type: List, value: {}, setting to null", body);
                        exchange.getIn().setBody(null);
                    }
                })
                .bean("itemProcessor", "processCategoryQuery")
                .bean("itemProcessor", "mapItemData")
                .multicast().parallelProcessing()
                .to("direct:writeTrendXml", "direct:writeReviewXml", "direct:writeStoreJson")
                .end()
                .end();

        from("direct:writeTrendXml")
                .routeId("writeTrendXml")
                .bean("itemProcessor", "prepareTrendXml")
                .choice()
                .when(body().isNotNull())
                .doTry()
                .marshal(trendXmlFormat)
                .to("file://{{app.output.item-trend-analyzer}}?fileExist=Override")
                .log(LoggingLevel.INFO, "Overwrote trend XML: ${header.CamelFileName}")
                .doCatch(Exception.class)
                .log(LoggingLevel.ERROR, "Failed to overwrite trend XML: ${header.CamelFileName}, error: ${exception.message}")
                .endDoTry()
                .endChoice()
                .when(body().isNull())
                .log(LoggingLevel.WARN, "Skipping null trend XML")
                .endChoice();

        from("direct:writeReviewXml")
                .routeId("writeReviewXml")
                .bean("itemProcessor", "prepareReviewXml")
                .choice()
                .when(body().isNotNull())
                .doTry()
                .marshal(reviewXmlFormat)
                .to("file://{{app.output.item-review-aggregator}}?fileExist=Override")
                .log(LoggingLevel.INFO, "Overwrote review XML: ${header.CamelFileName}")
                .doCatch(Exception.class)
                .log(LoggingLevel.ERROR, "Failed to overwrite review XML: ${header.CamelFileName}, error: ${exception.message}")
                .endDoTry()
                .endChoice()
                .when(body().isNull())
                .log(LoggingLevel.WARN, "Skipping null review XML")
                .endChoice();

        from("direct:writeStoreJson")
                .routeId("writeStoreJson")
                .bean("itemProcessor", "prepareStoreJson")
                .choice()
                .when(body().isNotNull())
                .doTry()
                .marshal(jsonFormat)
                .to("file://{{app.output.storefront-app}}?fileExist=Override")
                .log(LoggingLevel.INFO, "Overwrote store JSON: ${header.CamelFileName}")
                .doCatch(Exception.class)
                .log(LoggingLevel.ERROR, "Failed to overwrite store JSON: ${header.CamelFileName}, error: ${exception.message}")
                .endDoTry()
                .endChoice()
                .when(body().isNull())
                .log(LoggingLevel.WARN, "Skipping null store JSON")
                .endChoice();

        from("direct:updateControlRef")
                .routeId("updateControlRef")
                .bean("controlRefProcessor", "updateControlRef")
                .choice()
                .when(body().isNotNull())
                .to(mongoUri + "&collection={{app.control.collection}}&operation=save")
                .log(LoggingLevel.INFO, "controlRef updated with lastProcessTs: ${exchangeProperty.currentTs}")
                .endChoice()
                .when(body().isNull())
                .log(LoggingLevel.WARN, "Skipped controlRef update (null body)")
                .endChoice();
    }
}