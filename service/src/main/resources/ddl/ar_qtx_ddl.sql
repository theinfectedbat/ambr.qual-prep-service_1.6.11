define tsdata=TA_1566_DATA
define tsindex=TA_1566_INDX
--define tsdata=TRADE
--define tsindex=TRADE
--define tsdata=NKETA_DATA
--define tsindex=NKETA_INDX
--define tsdata=TEST_TABLE
--define tsindex=TEST_INDEX

DROP TABLE ar_qtx_work CASCADE CONSTRAINTS;

CREATE TABLE ar_qtx_work (
    qtx_wid        NUMBER,
    company_code   CHAR(50),
    priority       NUMBER,
    bom_key        NUMBER,
    iva_key        NUMBER,
    entity_key     NUMBER,
    entity_type    NUMBER,
    user_id        varchar2(250),
    time_stamp     timestamp
)
        STORAGE ( INITIAL 1024 M PCTINCREASE 50 )
    INITRANS 100 tablespace &tsdata;

create unique index uk_ar_qtx_work on ar_qtx_work(qtx_wid) STORAGE ( INITIAL 1024 M PCTINCREASE 50 )
    INITRANS 100 TABLESPACE  &&tsindex reverse parallel;
ALTER TABLE ar_qtx_work ADD CONSTRAINT pk_ar_qtx_work PRIMARY KEY ( qtx_wid )
    USING INDEX REVERSE tablespace &tsindex;

DROP TABLE ar_qtx_work_status CASCADE CONSTRAINTS;

CREATE TABLE ar_qtx_work_status (
    qtx_wid      NUMBER,
    status       NUMBER,
    time_stamp   timestamp
)
        STORAGE ( INITIAL 1024 M PCTINCREASE 50 )
    INITRANS 100 tablespace &tsdata;

create unique index uk_ar_qtx_work_status on ar_qtx_work_status(qtx_wid) STORAGE ( INITIAL 1024 M PCTINCREASE 50 )
    INITRANS 100 TABLESPACE  &&tsindex reverse parallel;
ALTER TABLE ar_qtx_work_status ADD CONSTRAINT pk_ar_qtx_work_status PRIMARY KEY ( qtx_wid )
    USING INDEX REVERSE tablespace &tsindex;

DROP TABLE ar_qtx_work_details CASCADE CONSTRAINTS;

CREATE TABLE ar_qtx_work_details (
    qtx_wid               NUMBER,
    qualtx_key            NUMBER,
    analysis_method       NUMBER,
    components            NUMBER,
    ctry_of_import        CHAR(3),
    reason_code           NUMBER,
    time_stamp            timestamp
)
        STORAGE ( INITIAL 1024 M PCTINCREASE 50 )
    INITRANS 100 tablespace &tsdata;

create unique index uk_ar_qtx_work_details on ar_qtx_work_details(qtx_wid) STORAGE ( INITIAL 1024 M PCTINCREASE 50 )
    INITRANS 100 TABLESPACE  &&tsindex reverse parallel;
ALTER TABLE ar_qtx_work_details ADD CONSTRAINT pk_ar_qtx_work_details PRIMARY KEY ( qtx_wid )
    USING INDEX REVERSE tablespace &tsindex;

DROP TABLE ar_qtx_work_hs CASCADE CONSTRAINTS;

CREATE TABLE ar_qtx_work_hs (
    qtx_wid          NUMBER,
    qtx_hspull_wid   NUMBER,
    status           NUMBER,
    ctry_cmpl_key    NUMBER,
    target_hs_ctry   CHAR(3),
    hs_number        CHAR(12),
    reason_code      NUMBER,
    time_stamp       timestamp
)
        STORAGE ( INITIAL 1024 M PCTINCREASE 50 )
    INITRANS 100 tablespace &tsdata;

create unique index uk_ar_qtx_work_hs on ar_qtx_work_hs(qtx_hspull_wid) STORAGE ( INITIAL 1024 M PCTINCREASE 50 )
    INITRANS 100 TABLESPACE  &&tsindex reverse parallel;
ALTER TABLE ar_qtx_work_hs ADD CONSTRAINT pk_ar_qtx_work_hs PRIMARY KEY ( qtx_hspull_wid )
    USING INDEX REVERSE tablespace &tsindex;
    
CREATE INDEX ar_qtx_work_hs_index_1 ON
    ar_qtx_work_hs ( qtx_wid )
        INITRANS 100 REVERSE TABLESPACE &tsindex;
        
DROP TABLE ar_qtx_comp_work CASCADE CONSTRAINTS;

CREATE TABLE ar_qtx_comp_work (
    qtx_comp_wid      NUMBER,
    qtx_wid           NUMBER,
    priority          NUMBER,
    bom_key           NUMBER,
    bom_comp_key      NUMBER,
    entity_key        NUMBER,
    entity_src_key    NUMBER,
    qualifier         NUMBER,
	qualtx_key        NUMBER,
    qualtx_comp_key   NUMBER,
    reason_code       NUMBER,
    time_stamp        timestamp
)
        STORAGE ( INITIAL 1024 M PCTINCREASE 50 )
    INITRANS 100 tablespace &tsdata;

create unique index uk_ar_qtx_comp_work on ar_qtx_comp_work(qtx_comp_wid) STORAGE ( INITIAL 1024 M PCTINCREASE 50 )
    INITRANS 100 TABLESPACE  &&tsindex reverse parallel;
ALTER TABLE ar_qtx_comp_work ADD CONSTRAINT pk_ar_qtx_comp_work PRIMARY KEY ( qtx_comp_wid )
    USING INDEX REVERSE tablespace &tsindex;

DROP TABLE ar_qtx_comp_work_status CASCADE CONSTRAINTS;

CREATE TABLE ar_qtx_comp_work_status (
    qtx_comp_wid   NUMBER,
    qtx_wid        NUMBER,
    status         NUMBER,
    time_stamp     timestamp
)
        STORAGE ( INITIAL 1024 M PCTINCREASE 50 )
    INITRANS 100 tablespace &tsdata;

create unique index uk_ar_qtx_comp_work_status on ar_qtx_comp_work_status(qtx_comp_wid) STORAGE ( INITIAL 1024 M PCTINCREASE 50 )
    INITRANS 100 TABLESPACE  &&tsindex reverse parallel;
ALTER TABLE ar_qtx_comp_work_status ADD CONSTRAINT pk_ar_qtx_comp_work_status PRIMARY KEY ( qtx_comp_wid )
    USING INDEX REVERSE tablespace &tsindex;

DROP TABLE ar_qtx_comp_work_hs CASCADE CONSTRAINTS;

CREATE TABLE ar_qtx_comp_work_hs (
    qtx_comp_hspull_wid     NUMBER,
    qtx_comp_wid     NUMBER,
    qtx_wid          NUMBER,
    status           NUMBER,
    ctry_cmpl_key    NUMBER,
    target_hs_ctry   CHAR(3),
    hs_number        CHAR(12),
    reason_code      NUMBER,
    time_stamp       timestamp
)
        STORAGE ( INITIAL 1024 M PCTINCREASE 50 )
    INITRANS 100 tablespace &tsdata;

create unique index uk_ar_qtx_comp_work_hs on ar_qtx_comp_work_hs(qtx_comp_hspull_wid) STORAGE ( INITIAL 1024 M PCTINCREASE 50 )
    INITRANS 100 TABLESPACE  &&tsindex reverse parallel;
ALTER TABLE ar_qtx_comp_work_hs ADD CONSTRAINT pk_ar_qtx_comp_work_hs PRIMARY KEY ( qtx_comp_hspull_wid )
    USING INDEX REVERSE tablespace &tsindex;

CREATE INDEX ar_qtx_comp_work_hs_index_1 ON
    ar_qtx_comp_work_hs ( qtx_comp_wid )
        INITRANS 100 REVERSE TABLESPACE &tsindex;

DROP TABLE ar_qtx_comp_work_iva CASCADE CONSTRAINTS;

CREATE TABLE ar_qtx_comp_work_iva (
    qtx_comp_iva_wid   NUMBER,
    qtx_comp_wid   NUMBER,
    qtx_wid        NUMBER,
    status         NUMBER,
    iva_key        NUMBER,
    reason_code    NUMBER,
    time_stamp     timestamp
)
        STORAGE ( INITIAL 1024 M PCTINCREASE 50 )
    INITRANS 100 tablespace &tsdata;

create unique index uk_ar_qtx_comp_work_iva on ar_qtx_comp_work_iva(qtx_comp_iva_wid) STORAGE ( INITIAL 1024 M PCTINCREASE 50 )
    INITRANS 100 TABLESPACE  &&tsindex reverse parallel;
ALTER TABLE ar_qtx_comp_work_iva ADD CONSTRAINT pk_ar_qtx_comp_work_iva PRIMARY KEY ( qtx_comp_iva_wid )
    USING INDEX REVERSE tablespace &tsindex;

CREATE INDEX ar_qtx_comp_work_iva_index_1 ON
    ar_qtx_comp_work_iva ( qtx_comp_wid )
        INITRANS 100 REVERSE TABLESPACE &tsindex;
        
DROP TABLE ar_qtx_work_log CASCADE CONSTRAINTS;

CREATE TABLE ar_qtx_work_log (
    qtx_wid       NUMBER,
    qtx_logid     NUMBER,
    entity_id     NUMBER,
    entity_name   NUMBER,
    error_log     clob,
    time_stamp    timestamp
)
        STORAGE ( INITIAL 1024 M PCTINCREASE 50 )
    INITRANS 100 tablespace &tsdata;

create unique index uk_ar_qtx_work_log on ar_qtx_work_log(qtx_logid) STORAGE ( INITIAL 1024 M PCTINCREASE 50 )
    INITRANS 100 TABLESPACE  &&tsindex reverse parallel;
ALTER TABLE ar_qtx_work_log ADD CONSTRAINT pk_ar_qtx_work_log PRIMARY KEY ( qtx_logid )
    USING INDEX REVERSE tablespace &tsindex;