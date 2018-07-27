CREATE TABLE "Sequence_Details" (
  "Accession" character varying(15) NOT NULL,
  "Definition" text,
  "Tax_ID" integer,
  "Organism" text NOT NULL,
  "Isolate" text,
  "Strain" text,
  "Collection_Date" text,
  "Itv_From" integer NOT NULL,
  "Itv_To" integer NOT NULL,
  "Comment" text,
  "pH1N1" boolean NOT NULL DEFAULT false, -- True if it is an Influenza A pH1N1 strain.
  CONSTRAINT "Accession_PKey" PRIMARY KEY ("Accession")
)
WITH (
  OIDS=FALSE
);

CREATE TABLE "Publication" (
  "Pub_ID" serial NOT NULL,
  "Pubmed_ID" integer ,
  "Pubmed_Central_ID" text,
  "Title" text,
  "Journal" text,
  CONSTRAINT "PK_Pub_ID" PRIMARY KEY ("Pub_ID")
)
WITH (
  OIDS=FALSE
);


CREATE TABLE "Author" (
	"Author_ID" serial NOT NULL,
	"First_Name" text,
	"Initials" text,
	"Last_Name" text,
  CONSTRAINT "PK_Author_Author_ID" PRIMARY KEY ("Author_ID")
)
WITH  (
  OIDS=FALSE
);

CREATE TABLE "Author_Publication" (
  "Author_ID" integer,
  "Pub_ID" integer,
  CONSTRAINT "PK_Author_Pub" PRIMARY KEY ("Author_ID","Pub_ID"),
  CONSTRAINT "AuthorPub_AuthorID_FKey" FOREIGN KEY ("Author_ID")
    REFERENCES "Author" ("Author_ID"),
  CONSTRAINT "AuthorPub_PubID_FKey" FOREIGN KEY ("Pub_ID")
    REFERENCES "Publication" ("Pub_ID")
)
WITH  (
  OIDS=FALSE
);

CREATE TABLE "Institution" (
  "Institution_ID" serial,
  "Institution" text,
  "Country" text,
  "City" text,
  "Latitude" real,
  "Longitude" real,
  CONSTRAINT "PK_Institution_Inst_ID" PRIMARY KEY ("Institution_ID")
)
WITH  (
  OIDS=FALSE
);

CREATE TABLE "Author_Institution" (
  "Author_ID" integer,
  "Institution_ID" integer,
  "Date" text,
  CONSTRAINT "PK_Author_Inst" PRIMARY KEY ("Author_ID","Institution_ID"),
  CONSTRAINT "AuthorInst_AuthorID_FKey" FOREIGN KEY ("Author_ID")
    REFERENCES "Author" ("Author_ID"),
  CONSTRAINT "AuthorInst_InstID_FKey" FOREIGN KEY ("Institution_ID")
    REFERENCES "Institution" ("Institution_ID")
)
WITH  (
  OIDS=FALSE
);

CREATE TABLE "Submission" (
  "Author_ID" integer,
  "Accession" character varying(15) NOT NULL,
  CONSTRAINT "PK_Submission" PRIMARY KEY ("Author_ID","Accession"),
  CONSTRAINT "Submission_AuthorID_FKey" FOREIGN KEY ("Author_ID")
    REFERENCES "Author" ("Author_ID")
)
WITH  (
  OIDS=FALSE
);

CREATE TABLE "Host" (
  "Accession" character varying(15),
  "Host_Name" text,
  "Host_taxon" integer,
  CONSTRAINT "HostAccession_FKey" FOREIGN KEY ("Accession")
      REFERENCES "Sequence_Details" ("Accession") MATCH SIMPLE
      ON UPDATE NO ACTION ON DELETE NO ACTION
)
WITH (
  OIDS=FALSE
);
CREATE INDEX host_accession_index
  ON "Host"
  USING btree
  ("Accession" COLLATE pg_catalog."default");

CREATE TABLE "Sequence" (
  "Accession" character varying(15),
  "Sequence" text NOT NULL,
  "Segment_Length" integer NOT NULL,
  CONSTRAINT "SeqAccession_FKey" FOREIGN KEY ("Accession")
      REFERENCES "Sequence_Details" ("Accession") MATCH SIMPLE
      ON UPDATE NO ACTION ON DELETE NO ACTION
)
WITH (
  OIDS=FALSE
);
CREATE INDEX sequence_long_accession_index
  ON "Sequence"
  USING btree
  ("Accession" COLLATE pg_catalog."default");

CREATE TABLE "Sequence_Publication" (
  "Accession" character varying(15),
  "Pub_ID" integer,
  CONSTRAINT "PubAccession_FKey" FOREIGN KEY ("Accession")
      REFERENCES "Sequence_Details" ("Accession") MATCH SIMPLE
      ON UPDATE NO ACTION ON DELETE NO ACTION,
  CONSTRAINT "PubmedID_FKey" FOREIGN KEY ("Pub_ID")
      REFERENCES "Publication" ("Pub_ID") MATCH SIMPLE
      ON UPDATE NO ACTION ON DELETE NO ACTION
)
WITH (
  OIDS=FALSE
);
CREATE INDEX "FKI_Pubmed_ID_Sequence_Publication"
  ON "Sequence_Publication"
  USING btree
  ("Pub_ID");
CREATE INDEX "index_accession"
  ON "Sequence_Publication"
  USING btree
  ("Accession" COLLATE pg_catalog."default");

CREATE TABLE "Location_GenBank" (
  "Accession" character varying(15),
  "Location" text,
  "Latitude" real,
  "Longitude" real,
  CONSTRAINT "LocationAccession_FKey" FOREIGN KEY ("Accession")
      REFERENCES "Sequence_Details" ("Accession") MATCH SIMPLE
      ON UPDATE NO ACTION ON DELETE NO ACTION
)
WITH (
  OIDS=FALSE
);
CREATE INDEX "locGB_accession_index"
  ON "Location_GenBank"
  USING btree
  ("Accession" COLLATE pg_catalog."default");

CREATE TABLE "Gene" (
  "Gene_ID" serial NOT NULL,
  "Accession" character varying(15) NOT NULL,
  "Gene_Name" text NOT NULL,
  "Normalized_Gene_Name" character varying(15),
  "Itv" text NOT NULL,
  CONSTRAINT "Gene_ID_PKEY" PRIMARY KEY ("Gene_ID"),
  CONSTRAINT "GeneAccession_FKey" FOREIGN KEY ("Accession")
      REFERENCES "Sequence_Details" ("Accession") MATCH SIMPLE
      ON UPDATE NO ACTION ON DELETE NO ACTION
)
WITH (
  OIDS=FALSE
);
CREATE INDEX "gene_accessionIndex"
  ON "Gene"
  USING btree
  ("Accession" COLLATE pg_catalog."default");

CREATE TABLE "Features" (
  "Feature_ID" serial NOT NULL,
  "Accession" character varying(15),
  "Header" text NOT NULL,
  "Position" text NOT NULL,
  "Key" text,
  "Value" text,
  CONSTRAINT "PK_Features" PRIMARY KEY ("Feature_ID"),
  CONSTRAINT "FeatureAccession_FKey" FOREIGN KEY ("Accession")
      REFERENCES "Sequence_Details" ("Accession") MATCH SIMPLE
      ON UPDATE NO ACTION ON DELETE NO ACTION
)
WITH (
  OIDS=FALSE
);
CREATE INDEX accession_feature_index
  ON "Features"
  USING btree
  ("Accession" COLLATE pg_catalog."default");

CREATE TABLE "Location_Geoname" (
  "Accession" character varying(15) NOT NULL,
  "Geoname_ID" integer,
  "Location" text,
  "Latitude" real,
  "Longitude" real,
  "Type" text,
  "Country" text,
  CONSTRAINT geoname_accession_fkey FOREIGN KEY ("Accession")
      REFERENCES "Sequence_Details" ("Accession") MATCH SIMPLE
      ON UPDATE NO ACTION ON DELETE NO ACTION
)
WITH (
  OIDS=FALSE
);
CREATE INDEX geoname_accession_index
  ON "Location_Geoname"
  USING btree
  ("Accession" COLLATE pg_catalog."default");

CREATE TABLE "Possible_Location"
(
  "Id" serial NOT NULL,
  "Accession" character varying(15),
  "Geoname_ID" integer,
  "Location" text,
  "Latitude" real,
  "Longitude" real,
  probability real,
  CONSTRAINT "PK_Id" PRIMARY KEY ("Id")
)
WITH (
  OIDS=FALSE
);
CREATE INDEX "possLoc_accession_index"
  ON "Possible_Location"
  USING btree
  ("Accession" COLLATE pg_catalog."default");

CREATE TABLE "Taxonomy_Concept" (
  node_id integer,
  name text,
  unique_name text,
  class text
)
WITH (
  OIDS=FALSE
);

CREATE TABLE "Taxonomy_Division" (
  division_id integer,
  code text,
  name text
)
WITH (
  OIDS=FALSE
);

CREATE TABLE "Taxonomy_Tree" (
  node_id integer,
  parent_node_id integer,
  rank text,
  code text,
  division integer
)
WITH (
  OIDS=FALSE
);

CREATE TABLE "Predictor"
(
  id serial NOT NULL,
  "USPS_code" text NOT NULL,
  "State" text NOT NULL,
  "Key" text NOT NULL,
  "Value" real,
  "Year" smallint,
  CONSTRAINT predictor_pkey PRIMARY KEY (id)
)
WITH (
  OIDS=FALSE
);
