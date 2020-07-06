## Dimension tables
### dim_cities
| FieldName | DataType | Description          |
|-----------|----------|----------------------|
| city_code | string   | City identifyer code |
| city_name | string   | City name            |

### dim_states
| FieldName  | DataType | Description   |
|------------|----------|---------------|
| state_name | string   | US state name |
| state_code | string   | US state code |

### dim_airports
| FieldName    | DataType | Description                                 |
|--------------|----------|---------------------------------------------|
| ident        | string   | Airport identifyer                          |
| type         | string   | Airport size                                |
| name         | string   | Name of airport                             |
| elevation_ft | float    | Airport elevation in feet                   |
| continent    | string   | Content where airport is located            |
| iso_country  | string   | ISO country code                            |
| iso_region   | string   | ISO region code                             |
| municipality | string   | Municipality wherein the airport is located |
| gps_code     | string   | GPS code for airport                        |
| iata_code    | string   | Three-letter geocode                        |
| port_code    | string   | Local airport code                          |
| coordinates  | string   | Lat/Long codes for airport location         |
 
### dim_demographics
| FieldName                               | DataType | Description                                                                            |
|-----------------------------------------|----------|----------------------------------------------------------------------------------------|
| state_code                              | string   | US state code                                                                          |
| state                                   | string   | US state name                                                                          |
| total_population                        | double   | Count of total population for US state                                                 |
| male_population                         | double   | Count of male population for US state                                                  |
| female_population                       | double   | Count of female population for US state                                                |
| american_indian_and_alaska_native       | bigint   | Count of American Indian or Alaskan Native population for US state                     |
| asian                                   | bigint   | Count of Asian population for US state                                                 |
| black_or_african_american               | bigint   | Count of Black or African American population for US state                             |
| hispanic_or_latino                      | bigint   | Count of Hispanic or Latino population for US state                                    |
| white                                   | bigint   | Count of White population for US state                                                 |
| male_population_ratio                   | double   | Ratio of male population to total population for US state                              |
| female_population_ratio                 | double   | Ratio of female population to total population for US state                            |
| american_indian_and_alaska_native_ratio | double   | Ratio of American Indian or Alaskan Native population to total population for US state |
| asian_ratio                             | double   | Ratio of Asian population to total population for US state                             |
| black_or_african_american_ratio         | double   | Ratio of Black or African American population to total population for US state         |
| hispanic_or_latino_ratio                | double   | Ratio of Hispanic or Latino population to total population for US state                |
| white_ratio                             | double   | Ratio of White population to total population for US state                             |
 
### dim_airlines
| FieldName  | DataType | Description                    |
|------------|----------|--------------------------------|
| airline_id | int      | Airline identifyer             |
| name       | string   | Airline name                   |
| alias      | string   | Airline alias                  |
| airline    | string   | Airline code                   |
| icao       | string   | ICAO code                      |
| call_sign  | string   | Airline name code              |
| country    | string   | Airline country                |
| active     | string   | Indicator if airline is active |

### dim_countries
| FieldName    | DataType | Description        |
|--------------|----------|--------------------|
| country_name | string   | Country name       |
| country_code | int      | Country identifyer |
 

### dim_visa_types
| FieldName | DataType | Description           |
|-----------|----------|-----------------------|
| visa_code | int      | Visa type code        |
| visa_type | string   | Visa type description |
 
### dim_entry_modes
| FieldName | DataType | Description                 |
|-----------|----------|-----------------------------|
| mode_name | string   | Entry mode description/name |
| mode_code | int      | Entry mode identifyer code  |

### dim_airport_codes
| FieldName    | DataType | Description             |
|--------------|----------|-------------------------|
| airport_code | string   | Airport code identifyer |
| airport_name | string   | Airtport name           |

## Facts table
### immigration_facts
| FieldName         | DataType | Description                                                 |
|-------------------|----------|-------------------------------------------------------------|
| cic_id            | int      | CIC ID number                                               |
| port_code         | string   | Airport code                                                |
| state_code        | string   | US state code                                               |
| visa_post         | string   | State deparment where the visa was issued                   |
| matflag           | string   | Flag indicating matching departure and arrival records      |
| dtaddto           | string   | Date up to when the individual is allowed to stay in the US |
| gender            | string   | Gender                                                      |
| airline           | string   | Airline code                                                |
| admnum            | double   | Admission number                                            |
| fltno             | string   | Airline flight number arriving in the US                    |
| visa_type         | string   | Admission class to stay in the US                           |
| mode_code         | int      | Code identifying mode of entry                              |
| orig_country_code | int      | Country of origin                                           |
| cit_country_code  | int      | City of origin                                              |
| year              | int      | Year                                                        |
| month             | int      | Month                                                       |
| birth_year        | int      | Birth year                                                  |
| age               | int      | Age                                                         |
| counter           | int      | Summary statistic counter                                   |
| arrival_date      | date     | Date of arrival in the US                                   |
| departure_date    | date     | Date of departure from the US                               |
| arrival_year      | string   | Arrival year                                                |
| arrival_month     | string   | Arrival month                                               |
| arrival_day       | string   | Arrival day                                                 |