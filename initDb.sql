CREATE TABLE addresses (
    name text NOT NULL PRIMARY KEY,
    id_osm int4 NOT NULL,
    lat float8 NOT NULL,
    lon float8 NOT NULL,
    dist_to_TG3 float4
);

CREATE TABLE job_listings (
    url text NOT NULL PRIMARY KEY,
    title text NOT NULL,
    company text NOT NULL,
    city VARCHAR(20),
    salaryFrom int2,
    salaryTo int2,
    salaryAvg int2,
    tags text,
    email VARCHAR(40),
    phone_no VARCHAR(20),
    address text REFERENCES addresses(name),
    link text,
    salaryType VARCHAR(5),
    remote bool,
    description_score int2 NOT NULL,
    distance_score int2 NOT NULL,
    combined_score int2 NOT NULL,
    entered timestamp NOT NULL
);