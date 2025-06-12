package org.example.model;
import org.example.model.ReviewXml;
import org.example.model.StoreJson;
import org.example.model.TrendXml;
import jakarta.xml.bind.JAXBElement;
import jakarta.xml.bind.annotation.XmlElementDecl;
import jakarta.xml.bind.annotation.XmlRegistry;

import javax.xml.namespace.QName;

@XmlRegistry
public class ObjectFactory {

    private static final QName _TrendXml_QNAME = new QName("", "TrendXml");
    private static final QName _ReviewXml_QNAME = new QName("", "ReviewXml");

    public ObjectFactory() {
    }

    public TrendXml createTrendXml() {
        return new TrendXml();
    }

    public ReviewXml createReviewXml() {
        return new ReviewXml();
    }

    public ReviewXml.Review createReviewXmlReview() {
        return new ReviewXml.Review();
    }

    @XmlElementDecl(namespace = "", name = "TrendXml")
    public JAXBElement<TrendXml> createTrendXmlElement(TrendXml value) {
        return new JAXBElement<>(_TrendXml_QNAME, TrendXml.class, null, value);
    }

    @XmlElementDecl(namespace = "", name = "ReviewXml")
    public JAXBElement<ReviewXml> createReviewXmlElement(ReviewXml value) {
        return new JAXBElement<>(_ReviewXml_QNAME, ReviewXml.class, null, value);
    }
}