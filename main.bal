import ballerina/http;
import ballerina/io;
import ballerina/sql;
import ballerinax/mysql;
import ballerinax/mysql.driver as _;

configurable string scraperApiUrl = ?;
configurable string scraperApiKey = ?;
configurable string dbName = ?;
configurable string dbHost = ?;
configurable int dbPort = ?;
configurable string dbUser = ?;
configurable string dbPass = ?;

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

type BankRecord record {
    int id;
    string name;
    string shortName;
};

type CurrencyRecord record {
    int id;
    string name;
    string symbol;
};

public function main() returns error? {
    http:Client scraperClient = check new (scraperApiUrl);
    io:println("scraperClient initialized");
    mysql:Client mysqlClient = check new (database = dbName, host = dbHost, port = dbPort, user = dbUser,
        password = dbPass
    );
    io:println("mysqlClient initialized");
    Metadata metadata = check scraperClient->/metadata({"API-Key": scraperApiKey});
    io:println("metadata fetched: " + metadata.toJsonString());
    foreach Data bank in metadata.banks {
        foreach Data currency in metadata.currencies {
            io:println("scraping for: " + bank.name + " " + currency.name);

            RateResponse rateResponse = check scraperClient->/scrape(bank = bank.id, currency = currency.id, 'type = "ttr");
            io:println("rateResponse :" + rateResponse.toJsonString());

            sql:ParameterizedQuery bankQuery = `SELECT * FROM bank WHERE shortName = ${bank.id} LIMIT 1;`;
            BankRecord bankRecord = check mysqlClient->queryRow(bankQuery);
            io:println("bankRecord :" + bankRecord.toJsonString());

            sql:ParameterizedQuery currencyQuery = `SELECT * FROM currency WHERE symbol = ${currency.id} LIMIT 1;`;
            CurrencyRecord currencyRecord = check mysqlClient->queryRow(currencyQuery);
            io:println("bankRecord :" + bankRecord.toJsonString());

            int bankId = bankRecord.id;
            int currencyId = currencyRecord.id;
            float rate = rateResponse.buyingRate;

            sql:ParameterizedQuery insertQuery = `INSERT INTO bank_rate (bank_id, currency_id, rate, date) VALUES (${bankId},${currencyId},${rate},now())`;
            _ = check mysqlClient->execute(insertQuery);
            io:println("rate added to db");
        }
    }

    check mysqlClient.close();

}
