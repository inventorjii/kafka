package com.kafka.kafkademo.model;

import java.util.Objects;

public class JoinedResult {
    public String catalog_number;
    public String country;
    public boolean is_selling;
    public String model;
    public String product_id;
    public String registration_id;
    public String registration_number;
    public String selling_status_date;
    public String order_number;
    public String quantity;
    public String sales_date;

    public JoinedResult() {}

    public JoinedResult(TopicValueA a, TopicValueB b) {
        this.catalog_number = a.catalog_number;
        this.country = a.country;
        this.is_selling = a.is_selling;
        this.model = a.model;
        this.product_id = a.product_id;
        this.registration_id = a.registration_id;
        this.registration_number = a.registration_number;
        this.selling_status_date = a.selling_status_date;
        this.order_number = b.order_number;
        this.quantity = b.quantity;
        this.sales_date = b.sales_date;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof JoinedResult)) return false;
        JoinedResult that = (JoinedResult) o;
        return is_selling == that.is_selling &&
                Objects.equals(catalog_number, that.catalog_number) &&
                Objects.equals(country, that.country) &&
                Objects.equals(model, that.model) &&
                Objects.equals(product_id, that.product_id) &&
                Objects.equals(registration_id, that.registration_id) &&
                Objects.equals(registration_number, that.registration_number) &&
                Objects.equals(selling_status_date, that.selling_status_date) &&
                Objects.equals(order_number, that.order_number) &&
                Objects.equals(quantity, that.quantity) &&
                Objects.equals(sales_date, that.sales_date);
    }

    @Override
    public int hashCode() {
        return Objects.hash(catalog_number, country, is_selling, model, product_id,
                registration_id, registration_number, selling_status_date,
                order_number, quantity, sales_date);
    }

    @Override
    public String toString() {
        return "JoinedResult{" +
                "catalog_number='" + catalog_number + '\'' +
                ", country='" + country + '\'' +
                ", is_selling=" + is_selling +
                ", model='" + model + '\'' +
                ", product_id='" + product_id + '\'' +
                ", registration_id='" + registration_id + '\'' +
                ", registration_number='" + registration_number + '\'' +
                ", selling_status_date='" + selling_status_date + '\'' +
                ", order_number='" + order_number + '\'' +
                ", quantity='" + quantity + '\'' +
                ", sales_date='" + sales_date + '\'' +
                '}';
    }
}