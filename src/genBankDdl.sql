CREATE TABLE "Sequence_Details" (
  "Accession" text NOT NULL,
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
  "Pub_ID" bigserial NOT NULL,
  "Pubmed_ID" integer NOT NULL,
  "Pubmed_Central_ID" text,
  "Authors" text NOT NULL,
  "Title" text NOT NULL,
  "Journal" text,
  CONSTRAINT "PK_Pub_ID" PRIMARY KEY ("Pub_ID"),
  CONSTRAINT "Pubmed_Unique" UNIQUE ("Pubmed_ID")
)
WITH (
  OIDS=FALSE
);

CREATE TABLE "Host" (
  "Accession" text,
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
  "Accession" text,
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
  "Accession" text,
  "Pub_ID" integer,
  CONSTRAINT "PubAccession_FKey" FOREIGN KEY ("Accession")
      REFERENCES "Sequence_Details" ("Accession") MATCH SIMPLE
      ON UPDATE NO ACTION ON DELETE NO ACTION,
  CONSTRAINT "PubmedID_FKey" FOREIGN KEY ("Pub_ID")
      REFERENCES "Publication" ("Pubmed_ID") MATCH SIMPLE
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
  "Accession" text,
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
  "Gene_ID" bigserial NOT NULL,
  "Accession" text NOT NULL,
  "Gene_Name" text NOT NULL,
  "Normalized_Gene_Name" text,
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
  "Feature_ID" bigserial NOT NULL,
  "Accession" text,
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
  "Accession" text NOT NULL,
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
  "Id" bigserial NOT NULL,
  "Accession" text,
  "Geoname_ID" integer,
  "Location" text,
  "Latitude" real,
  "Longitude" real,
  probability double precision,
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
  node_id bigint,
  name text,
  unique_name text,
  class text
)
WITH (
  OIDS=FALSE
);

CREATE TABLE "Taxonomy_Division" (
  division_id bigint,
  code text,
  name text
)
WITH (
  OIDS=FALSE
);

CREATE TABLE "Taxonomy_Tree" (
  node_id bigint,
  parent_node_id bigint,
  rank text,
  code text,
  division bigint
)
WITH (
  OIDS=FALSE
);

CREATE TABLE "Predictor"
(
  id bigserial NOT NULL,
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