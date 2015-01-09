CREATE TABLE PART (
        partkey      INT,
        name         VARCHAR(55),
        mfgr         CHAR(25),
        brand        CHAR(10),
        type         VARCHAR(25),
        size         INT,
        container    CHAR(10),
        retailprice  DECIMAL,
        comment      VARCHAR(23)
    );
CREATE TABLE PARTSUPP (
        partkey      INT,
        suppkey      INT,
        availqty     INT,
        supplycost   DECIMAL,
        comment      VARCHAR(199)
    );
select part.brand, part.type, part.size, count(distinct partsupp.suppkey) as suppliercount
from partsupp, part
where part.partkey = partsupp.partkey and part.brand <> 'Brand#12'
group by part.brand, part.type, part.size
order by suppliercount, part.brand, part.type, part.size;
