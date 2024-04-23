import ballerina/http;
import ballerina/sql;
import ballerinax/mysql;
import ballerinax/mysql.driver as _;

configurable string scraperApiUrl = ?;
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
    mysql:Client mysqlClient = check new (database = dbName, host = dbHost, port = dbPort, user = dbUser,
        password = dbPass
    );

    Metadata metadata = check scraperClient->/metadata;

    foreach Data bank in metadata.banks {
        foreach Data currency in metadata.currencies {
            RateResponse rateResponse = check scraperClient->/scrape(bank = bank.id, currency = currency.id, 'type = "ttr");

            sql:ParameterizedQuery bankQuery = `SELECT * FROM bank WHERE shortName = ${bank.id} LIMIT 1;`;
            BankRecord bankRecord = check mysqlClient->queryRow(bankQuery);

            sql:ParameterizedQuery currencyQuery = `SELECT * FROM currency WHERE symbol = ${currency.id} LIMIT 1;`;
            CurrencyRecord currencyRecord = check mysqlClient->queryRow(currencyQuery);

            int bankId = bankRecord.id;
            int currencyId = currencyRecord.id;
            float rate = rateResponse.buyingRate;

            sql:ParameterizedQuery insertQuery = `INSERT INTO bank_rate (bank_id, currency_id, rate, date) VALUES (${bankId},${currencyId},${rate},now())`;
            _ = check mysqlClient->execute(insertQuery);
        }
    }

    check mysqlClient.close();

}
