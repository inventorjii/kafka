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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        JoinedResult that = (JoinedResult) o;
        return Objects.equals(catalog_number, that.catalog_number) &&
                Objects.equals(country, that.country) &&
                Objects.equals(is_selling, that.is_selling) &&
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


}