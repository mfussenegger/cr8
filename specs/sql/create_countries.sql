create table countries (
    "areaInSqKm" float,
    capital string,
    continent string,
    "continentName" string,
    "countryCode" string,
    "countryName" string,
    "currencyCode" string,
    east float,
    "fipsCode" string,
    "isoAlpha3" string,
    "isoNumeric" string,
    languages string,
    north float,
    population integer,
    south float,
    west float
) with (number_of_replicas = 0);
