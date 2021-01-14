-- AUTHOR KONSTANTIN TIZ

DROP INDEX edge_idx;

DROP TABLE IF EXISTS edges CASCADE;
DROP TABLE IF EXISTS edgebody CASCADE;
DROP TABLE IF EXISTS nodes CASCADE;

CREATE TABLE nodes (
  id 		INT	 		NOT NULL,
  firstname VARCHAR(20)	NOT NULL,
  lastname 	VARCHAR(20)	NOT NULL,
  gender 	VARCHAR(7) 	NOT NULL,
  birth 	DATE 		NOT NULL,
  country 	VARCHAR(2) 	NOT NULL,
  PRIMARY KEY (id)
);


CREATE TABLE edgebody (
  id 	INT	 	    NOT NULL,
  type 	VARCHAR(8)  NOT NULL,
  since	DATE 		NOT NULL,
  PRIMARY KEY (id)
);

CREATE TABLE edges (
	src		INT	NOT NULL,
	dst		INT	NOT NULL,
	body	INT	NOT NULL,
	PRIMARY KEY(src, dst),
	FOREIGN KEY (src) REFERENCES nodes(id),
	FOREIGN KEY (dst) REFERENCES nodes(id),
	FOREIGN KEY (body)REFERENCES edgebody(id)
);

CREATE INDEX edge_idx ON edges(src);
