create table if not exists marketing (
    age tinyint,
    sex tinyint,
    rate smallint,
    family_status varchar(15),
    n_children tinyint,
    second_car boolean)
    row format delimited
    fields terminated by ','
    stored as textfile
    tblproperties('skip.header.line.count'='1');

create table if not exists immatriculations (
    immatriculation varchar(10),
    brand varchar(255),
    model varchar(255),
    power smallint,
    length string,
    n_places tinyint,
    n_doors tinyint,
    color varchar(255),
    occasion boolean,
    price int)
    row format delimited
    fields terminated by ','
    stored as textfile
    tblproperties('skip.header.line.count'='1');

create table if not exists catalogue (
    brand varchar(255),
    model varchar(255),
    power smallint,
    length varchar(15),
    n_places tinyint,
    n_doors tinyint,
    colors varchar(255),
    occasion boolean,
    price int)
    row format delimited
    fields terminated by ','
    stored as textfile
    tblproperties('skip.header.line.count'='1');

create table if not exists clients_14 (
    age tinyint,
    sex tinyint,
    rate smallint,
    family_status varchar(255),
    n_children tinyint,
    second_car boolean,
    immatriculation varchar(10))
    row format delimited
    fields terminated by ','
    stored as textfile
    tblproperties('skip.header.line.count'='1');

create table if not exists clients_19 (
    age tinyint,
    sex tinyint,
    rate smallint,
    family_status varchar(255),
    n_children tinyint,
    second_car boolean,
    immatriculation varchar(10))
    row format delimited
    fields terminated by ','
    stored as textfile
    tblproperties('skip.header.line.count'='1');
