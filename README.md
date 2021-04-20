# Big Data Management 
## Project - Data Lake Design 

### Project goal 

Descriptive and predictive analysis of data related to Barcelona’s housing and the relationship with its economy.

### Part 1 goals (Data design)

+ Propose and deploy the right kind of storage, data model and structure for each dataset
+ Conceptualize the data processing pipelines required for the integration

### Datasets description

+ **D1: Barcelona rentals**

[Idealista](https://www.idealista.com/en/) is a portal where home owners can advertise their places to be rented or sold. Data was downloaded from their Search API as JSON documents (see the API's reference for more details on the schema of the response).

+ **D2: Territorial distribution of income**

The second source of data was obtained from Barcelona's Open Data portal [Open Data BCN](https://opendata-ajuntament.barcelona.cat/data/en/dataset/est-renda-familiar). Precisely, the data set contains the territorial income distribution in the city of Barcelona at the neighborhood level. Precisely, you will encounter 11 datasets (each corresponding to one year in the range 2007-2017), where for each neighborhood and year they provide its population and RFD index (family income index).

+ **Lookup Tables**

In order to achieve our goal you must effectively cross D1 and D2. However, note that such different data sources have been generated independently, and thus they cannot be joined directly by value. To that end, the lecturers performed a data reconcialiation process to provide a mapping between D1 and D2. Data reconciliation is a necessary step when joining heterogeneous datasets that refer to the same real-world concept but have different values that refer to them. Here, they provide us with two lookup tables (one for D1, and one for D2), that map each distinct value for neighborhood and district to its corresponding Wikidata ID.
