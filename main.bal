import ballerina/http;
import ballerina/io;

configurable string scraperApiUrl = ?;

type Data record {
    string id;
    string name;
};

type Metadata record {
    Data[] banks;
    Data[] currencies;
    Data[] types;
};

type RateResponse record {
    Data bank;
    Data currency;
    string 'type;
    float buyingRate;
    float sellingRate;
};

public function main() returns error? {
    http:Client scraperClient = check new (scraperApiUrl);

    Metadata metadata = check scraperClient->/metadata;

    foreach Data bank in metadata.banks {

        foreach Data currency in metadata.currencies {
            RateResponse rateResponse = check scraperClient->/scrape(bank = bank.id, currency = currency.id, 'type = "ttr");
            io:println(rateResponse.toJsonString());

        }

    }

}
