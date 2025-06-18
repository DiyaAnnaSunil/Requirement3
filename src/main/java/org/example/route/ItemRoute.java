package org.example.route;

import org.apache.camel.LoggingLevel;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.jackson.JacksonDataFormat;
import org.apache.camel.component.mongodb.MongoDbConstants;
import org.apache.camel.converter.jaxb.JaxbDataFormat;
import org.example.model.ReviewXml;
import org.example.model.StoreJson;
import org.example.model.TrendXml;
import org.example.processor.ItemProcessor;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class ItemRoute extends RouteBuilder {
    private static final Logger logger = LoggerFactory.getLogger(ItemRoute.class);

    @Override
    public void configure() throws Exception {
        // Global exception handling
        onException(Exception.class)
                .handled(true)
                .log(LoggingLevel.ERROR, "Route failed: ${exception.message}, stacktrace: ${exception.stacktrace}, itemId: ${exchangeProperty.itemId}, currentTs: ${exchangeProperty.currentTs}")
                .stop();

        JaxbDataFormat trendXmlFormat = new JaxbDataFormat(TrendXml.class.getPackage().getName());
        JaxbDataFormat reviewXmlFormat = new JaxbDataFormat(ReviewXml.class.getPackage().getName());
        JacksonDataFormat jsonFormat = new JacksonDataFormat(StoreJson.class);

        String mongoUri = "mongodb:mongoDbComponent?database={{app.mongodb.database}}";

        from("quartz://fileExport?cron={{app.scheduler.cron}}&stateful=true")
                .routeId("fileExport")
                .bean("itemProcessor", "setCurrentTimestamp")
                .log(LoggingLevel.DEBUG, "After setCurrentTimestamp, currentTs: ${exchangeProperty.currentTs}")
                .to("direct:fetchControlRef")
                .log(LoggingLevel.DEBUG, "Before processItems, currentTs: ${exchangeProperty.currentTs}")
                .to("direct:processItems")
                .log(LoggingLevel.DEBUG, "After processItems, currentTs: ${exchangeProperty.currentTs}")
                .to("direct:updateControlRef")
                .log(LoggingLevel.INFO, "File export completed");

        from("direct:fetchControlRef")
                .routeId("fetchControlRef")
                .log(LoggingLevel.DEBUG, "Before fetching controlRef")
                .setBody(constant(new Document("_id", "global")))
                .log(LoggingLevel.DEBUG, "Set query for controlRef: ${body}")
                .to(mongoUri + "&collection={{app.control.collection}}&operation=findOneByQuery&outputType=Document")
                .bean("controlRefProcessor", "fetchControlRefs")
                .log(LoggingLevel.INFO, "Fetched controlRefMap with ${exchangeProperty.controlRefMap.size()} entries");

        from("direct:processItems")
                .routeId("processItems")
                .doTry()
                .bean("itemProcessor", "prepareItemQuery")
                .log(LoggingLevel.DEBUG, "After prepareItemQuery, query: ${body}")
                .to(mongoUri + "&collection={{app.item.collection}}&operation=findAll")
                .bean("itemProcessor", "validateItemList")
                .bean("itemProcessor", "filterValidItems")
                .bean("itemProcessor", "logFetchedItems")
                .choice()
                .when(simple("${body} != null && ${body.size()} > 0"))
                .split(body())
                .log(LoggingLevel.DEBUG, "Processing item ${exchangeProperty.itemId}")
                .bean("itemProcessor", "enrichWithCategory")
                .log(LoggingLevel.DEBUG, "Executing category query for item ${exchangeProperty.itemId}, query: ${body}")
                .to(mongoUri + "&collection={{app.category.collection}}&operation=findOneByQuery&outputType=Document")
                .bean("itemProcessor", "validateCategoryResult")
                .bean("itemProcessor", "processCategoryQuery")
                .bean("itemProcessor", "mapItemData")
                .to("direct:writeTrendXml")
                .to("direct:writeReviewXml")
                .to("direct:writeStoreJson")
                .log(LoggingLevel.DEBUG, "Completed file writes for item ${exchangeProperty.itemId}")
                .end()
                .log(LoggingLevel.DEBUG, "Completed processItems split, currentTs: ${exchangeProperty.currentTs}")
                .endChoice()
                .otherwise()
                .log(LoggingLevel.INFO, "No valid items to process, skipping split")
                .endChoice()
                .endDoTry()
                .doCatch(Exception.class)
                .log(LoggingLevel.ERROR, "Failed processing items: ${exception.message}, currentTs: ${exchangeProperty.currentTs}")
                .end();

        from("direct:writeTrendXml")
                .routeId("writeTrendXml")
                .bean("itemProcessor", "prepareTrendXml")
                .choice()
                .when(simple("${body} != null"))
                .doTry()
                .marshal(trendXmlFormat)
                .setHeader("OutputFolder", constant("trend"))
                .bean("itemProcessor", "checkFileExistence")
                .to("file://{{app.output.item-trend-analyzer}}?fileName=${header.CamelFileName}&fileExist=Override")
                .choice()
                .when(simple("${exchangeProperty.fileExisted} == true"))
                .log(LoggingLevel.INFO, "Overwrote file: ${header.CamelFileName}")
                .when(simple("${exchangeProperty.fileExisted} == false"))
                .log(LoggingLevel.INFO, "Created new file: ${header.CamelFileName}")
                .endChoice()
                .endDoTry()
                .doCatch(Exception.class)
                .log(LoggingLevel.INFO, "Failed to overwrite trend XML: ${header.CamelFileName}, error: ${exception.message}")
                .end()
                .endChoice();

        from("direct:writeReviewXml")
                .routeId("writeReviewXml")
                .bean("itemProcessor", "prepareReviewXml")
                .choice()
                .when(simple("${body} != null"))
                .doTry()
                .marshal(reviewXmlFormat)
                .setHeader("OutputFolder", constant("review"))
                .bean("itemProcessor", "checkFileExistence")
                .to("file://{{app.output.item-review-aggregator}}?fileName=${header.CamelFileName}&fileExist=Override")
                .choice()
                .when(simple("${exchangeProperty.fileExisted} == true"))
                .log(LoggingLevel.INFO, "Overwrote file: ${header.CamelFileName}")
                .when(simple("${exchangeProperty.fileExisted} == false"))
                .log(LoggingLevel.INFO, "Created new file: ${header.CamelFileName}")
                .endChoice()
                .endDoTry()
                .doCatch(Exception.class)
                .log(LoggingLevel.INFO, "Failed to overwrite review XML: ${header.CamelFileName}, error: ${exception.message}")
                .end()
                .endChoice();

        from("direct:writeStoreJson")
                .routeId("writeStoreJson")
                .bean("itemProcessor", "prepareStoreJson")
                .choice()
                .when(simple("${body} != null"))
                .doTry()
                .marshal(jsonFormat)
                .setHeader("OutputFolder", constant("store"))
                .bean("itemProcessor", "checkFileExistence")
                .to("file://{{app.output.storefront-app}}?fileName=${header.CamelFileName}&fileExist=Override")
                .choice()
                .when(simple("${exchangeProperty.fileExisted} == true"))
                .log(LoggingLevel.INFO, "Overwrote file: ${header.CamelFileName}")
                .when(simple("${exchangeProperty.fileExisted} == false"))
                .log(LoggingLevel.INFO, "Created new file: ${header.CamelFileName}")
                .endChoice()
                .endDoTry()
                .doCatch(Exception.class)
                .log(LoggingLevel.INFO, "Failed to overwrite store JSON: ${header.CamelFileName}, error: ${exception.message}")
                .end()
                .endChoice();

        from("direct:updateControlRef")
                .routeId("updateControlRef")
                .log(LoggingLevel.DEBUG, "Starting controlRef update with currentTs: ${exchangeProperty.currentTs}")
                .bean("controlRefProcessor", "updateControlRef")
                .choice()
                .when(simple("${body} != null"))
                .doTry()
                .to(mongoUri + "&collection={{app.control.collection}}&operation=save")
                .log(LoggingLevel.INFO, "controlRef updated with lastProcessTs: ${exchangeProperty.currentTs}")
                .endDoTry()
                .doCatch(Exception.class)
                .log(LoggingLevel.ERROR, "Failed to save controlRef to MongoDB: ${exception.message}")
                .end()
                .endChoice();
    }
}