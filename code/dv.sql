-- dv.sql 
--
-- CROSS JOIN JSON_TABLE ... JOIN base_table returns 0 rows.
-- The identical query with /*+ NO_MERGE(dv) */ returns the expected 1 row.
--

SET ECHO ON
SET FEEDBACK ON
SET PAGESIZE 50
SET LINESIZE 120

-- -- Setup ---------------------------------------------------------------------

DROP VIEW  trade_dv;
DROP TABLE trade_legs PURGE;
DROP TABLE trades     PURGE;

CREATE TABLE trades (
    trade_id  NUMBER       PRIMARY KEY,
    trade_ref VARCHAR2(30) NOT NULL
);

CREATE TABLE trade_legs (
    leg_id     NUMBER    PRIMARY KEY,
    trade_id   NUMBER    NOT NULL REFERENCES trades(trade_id) ON DELETE CASCADE,
    leg_number NUMBER(2) NOT NULL,
    leg_type   VARCHAR2(20)
);

CREATE INDEX trade_legs_trade_id_ix ON trade_legs(trade_id);

CREATE OR REPLACE JSON RELATIONAL DUALITY VIEW trade_dv AS
  trades @insert @update @delete {
    _id      : trade_id,
    tradeRef : trade_ref,
    legs : trade_legs @insert @update @delete {
      legId      : leg_id,
      legNumber  : leg_number,
      legType    : leg_type
    }
  };

-- -- Populate ------------------------------------------------------------------

INSERT INTO trades     (trade_id, trade_ref)                      VALUES (1, 'T-001');
INSERT INTO trade_legs (leg_id, trade_id, leg_number, leg_type)   VALUES (1, 1, 1, 'LONG');
COMMIT;



-- -- Query A: no hint -- expected 1 row, returns 0 (bug) -----------------

SELECT jt.dv_leg_number, jt.dv_leg_type,
       l.leg_number     AS rel_leg_number,
       l.leg_type       AS rel_leg_type
FROM  (SELECT data FROM trade_dv
       WHERE  json_value(data, '$._id' RETURNING NUMBER) = 1) dv
CROSS JOIN JSON_TABLE(dv.data, '$.legs[*]' COLUMNS (
    dv_leg_number NUMBER       PATH '$.legNumber',
    dv_leg_type   VARCHAR2(20) PATH '$.legType')) jt
JOIN trade_legs l ON l.trade_id = 1 AND l.leg_number = jt.dv_leg_number;



-- -- Query B: NO_MERGE hint -- expected 1 row -----------------------------------

SELECT /*+ NO_MERGE(dv) */
       jt.dv_leg_number, jt.dv_leg_type,
       l.leg_number     AS rel_leg_number,
       l.leg_type       AS rel_leg_type
FROM  (SELECT data FROM trade_dv
       WHERE  json_value(data, '$._id' RETURNING NUMBER) = 1) dv
CROSS JOIN JSON_TABLE(dv.data, '$.legs[*]' COLUMNS (
    dv_leg_number NUMBER       PATH '$.legNumber',
    dv_leg_type   VARCHAR2(20) PATH '$.legType')) jt
JOIN trade_legs l ON l.trade_id = 1 AND l.leg_number = jt.dv_leg_number;
