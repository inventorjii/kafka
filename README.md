
## Overview

This application is a Kafka Streams-based service that reads messages from two Kafka topics (`topicA` and `topicB`), performs filtering and validation, joins the data based on a composite key (`catalog_number` and `country`), and outputs the enriched result into a third topic (`topicC`).

## Features

- Filters invalid records based on:
    - Country must be `"001"`
    - `catalog_number` must be exactly 5 characters
    - Dates must follow `yyyy-MM-dd'T'HH:mm:ss.SSSSSSZ` format
- Joins registration data from `topicA` with sales data from `topicB`
- Emits only the final joined result using Kafka Streams `suppress()`
- Writes final output to `topicC`

## Deatils

- Main logic for joining two records from 2 topics is present in - `StreamProcessor.java` file in `config` folder.
- I am adding records to topic A and B via rest API present in `controller` folder.
- Classes present in model folder are DTO classes.
- other classes and packages are helper packages.


## Technologies used

- Java 17
- Spring Boot 3.x
- Kafka and Kafka Streams

## Results

TOPIC_A data - 

{
"key": {
"catalog_number": "29525",
"country": "001"
},
"value": {
"catalog_number": "29525",
"is_selling": true,
"model": "29525",
"product_id": "int7218",
"registration_id": "int4123",
"registration_number": "REG03814",
"selling_status_date": "2023-06-30T18:21:31.000000Z",
"country": "001"
},
"audit": {
"event_name": "Registration",
"source_system": "RGR"
}
}

TOPIC_B data - 

{
"key": {
"catalog_number": "29525",
"country": "001"
},
"value": {
"catalog_number": "29525",
"order_number": "03814",
"quantity": "2",
"sales_date": "2023-07-30T18:21:31.000000Z",
"country": "001"
},
"audit": {
"event_name": "Sales Event",
"source_system": "SLS"
}
}

TOPIC_C data -
{
"catalog_number": "29525",
"country": "001",
"is_selling": true,
"model": "29525",
"product_id": "int7218",
"registration_id": "int4123",
"registration_number": "REG03814",
"selling_status_date": "2023-06-30T18:21:31.000000Z",
"order_number": "03814",
"quantity": "2",
"sales_date": "2023-07-30T18:21:31.000000Z"
}
