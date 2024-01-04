drop table catalogue;

create table if not exists catalogue (
    brand varchar(255),
    model varchar(255),
    power smallint,
    length varchar(15),
    n_places tinyint,
    n_doors tinyint,
    colors varchar(255),
    occasion boolean,
    price int,
    avg_bonus_malus float,
    avg_rejets_co2 float,
    avg_cout_energie float)
    row format delimited
    fields terminated by ','
    stored as textfile
    tblproperties('skip.header.line.count'='1');

load data inpath "/root/tpa/tpa/DATABASE/HDFS/final_df.csv/*.csv" into table catalogue;