--
-- PostgreSQL database dump
--

-- Dumped from database version 16.4 (Ubuntu 16.4-1.pgdg22.04+1)
-- Dumped by pg_dump version 16.4 (Ubuntu 16.4-1.pgdg22.04+1)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: public; Type: SCHEMA; Schema: -; Owner: dimus
--

-- *not* creating schema, since initdb creates it


ALTER SCHEMA public OWNER TO dimus;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: canonical_fulls; Type: TABLE; Schema: public; Owner: dimus
--

CREATE TABLE public.canonical_fulls (
    id uuid NOT NULL,
    name character varying(255) NOT NULL COLLATE pg_catalog."C"
);


ALTER TABLE public.canonical_fulls OWNER TO dimus;

--
-- Name: canonical_stems; Type: TABLE; Schema: public; Owner: dimus
--

CREATE TABLE public.canonical_stems (
    id uuid NOT NULL,
    name character varying(255) NOT NULL COLLATE pg_catalog."C"
);


ALTER TABLE public.canonical_stems OWNER TO dimus;

--
-- Name: canonicals; Type: TABLE; Schema: public; Owner: dimus
--

CREATE TABLE public.canonicals (
    id uuid NOT NULL,
    name character varying(255) NOT NULL COLLATE pg_catalog."C"
);


ALTER TABLE public.canonicals OWNER TO dimus;

--
-- Name: data_sources; Type: TABLE; Schema: public; Owner: dimus
--

CREATE TABLE public.data_sources (
    id smallint NOT NULL,
    uuid uuid DEFAULT '00000000-0000-0000-0000-000000000000'::uuid,
    title character varying(255),
    title_short character varying(50),
    version character varying(50),
    revision_date text,
    doi character varying(50),
    citation text,
    authors text,
    description text,
    website_url character varying(255),
    data_url character varying(255),
    outlink_url text,
    is_outlink_ready boolean,
    is_curated boolean,
    is_auto_curated boolean,
    has_taxon_data boolean,
    record_count integer,
    updated_at timestamp without time zone
);


ALTER TABLE public.data_sources OWNER TO dimus;

--
-- Name: name_string_indices; Type: TABLE; Schema: public; Owner: dimus
--

CREATE TABLE public.name_string_indices (
    data_source_id integer NOT NULL,
    record_id character varying(255) NOT NULL,
    name_string_id uuid NOT NULL,
    outlink_id character varying(255),
    global_id character varying(255),
    local_id character varying(255),
    code_id smallint,
    rank character varying(255),
    accepted_record_id character varying(255),
    classification text,
    classification_ids text,
    classification_ranks text
);


ALTER TABLE public.name_string_indices OWNER TO dimus;

--
-- Name: name_strings; Type: TABLE; Schema: public; Owner: dimus
--

CREATE TABLE public.name_strings (
    id uuid NOT NULL,
    name character varying(500) NOT NULL COLLATE pg_catalog."C",
    year integer,
    cardinality integer,
    canonical_id uuid,
    canonical_full_id uuid,
    canonical_stem_id uuid,
    virus boolean,
    bacteria boolean DEFAULT false NOT NULL,
    surrogate boolean,
    parse_quality integer DEFAULT 0 NOT NULL
);


ALTER TABLE public.name_strings OWNER TO dimus;

--
-- Name: verification; Type: MATERIALIZED VIEW; Schema: public; Owner: dimus
--

CREATE MATERIALIZED VIEW public.verification AS
 WITH taxon_names AS (
         SELECT nsi_1.data_source_id,
            nsi_1.record_id,
            nsi_1.name_string_id,
            ns_1.name
           FROM (public.name_string_indices nsi_1
             JOIN public.name_strings ns_1 ON ((nsi_1.name_string_id = ns_1.id)))
        )
 SELECT nsi.data_source_id,
    nsi.record_id,
    nsi.name_string_id,
    ns.name,
    ns.year,
    ns.cardinality,
    ns.canonical_id,
    ns.virus,
    ns.bacteria,
    ns.parse_quality,
    nsi.local_id,
    nsi.outlink_id,
    nsi.accepted_record_id,
    tn.name_string_id AS accepted_name_id,
    tn.name AS accepted_name,
    nsi.classification,
    nsi.classification_ranks,
    nsi.classification_ids
   FROM ((public.name_string_indices nsi
     JOIN public.name_strings ns ON ((ns.id = nsi.name_string_id)))
     LEFT JOIN taxon_names tn ON (((nsi.data_source_id = tn.data_source_id) AND ((nsi.accepted_record_id)::text = (tn.record_id)::text))))
  WHERE (((ns.canonical_id IS NOT NULL) AND (ns.surrogate <> true) AND ((ns.bacteria <> true) OR (ns.parse_quality < 3))) OR (ns.virus = true))
  WITH NO DATA;


ALTER MATERIALIZED VIEW public.verification OWNER TO dimus;

--
-- Name: vernacular_string_indices; Type: TABLE; Schema: public; Owner: dimus
--

CREATE TABLE public.vernacular_string_indices (
    data_source_id integer NOT NULL,
    record_id character varying(255) NOT NULL,
    vernacular_string_id uuid NOT NULL,
    language character varying(100),
    lang_code character varying(3),
    locality character varying(100),
    country_code character varying(50)
);


ALTER TABLE public.vernacular_string_indices OWNER TO dimus;

--
-- Name: vernacular_strings; Type: TABLE; Schema: public; Owner: dimus
--

CREATE TABLE public.vernacular_strings (
    id uuid NOT NULL,
    name character varying(255) NOT NULL COLLATE pg_catalog."C"
);


ALTER TABLE public.vernacular_strings OWNER TO dimus;

--
-- Name: word_name_strings; Type: TABLE; Schema: public; Owner: dimus
--

CREATE TABLE public.word_name_strings (
    word_id uuid NOT NULL,
    name_string_id uuid NOT NULL,
    canonical_id uuid
);


ALTER TABLE public.word_name_strings OWNER TO dimus;

--
-- Name: words; Type: TABLE; Schema: public; Owner: dimus
--

CREATE TABLE public.words (
    id uuid NOT NULL,
    normalized character varying(255) NOT NULL COLLATE pg_catalog."C",
    modified character varying(255) NOT NULL COLLATE pg_catalog."C",
    type_id integer
);


ALTER TABLE public.words OWNER TO dimus;

--
-- Name: canonical_fulls canonical_fulls_pkey; Type: CONSTRAINT; Schema: public; Owner: dimus
--

ALTER TABLE ONLY public.canonical_fulls
    ADD CONSTRAINT canonical_fulls_pkey PRIMARY KEY (id);


--
-- Name: canonical_stems canonical_stems_pkey; Type: CONSTRAINT; Schema: public; Owner: dimus
--

ALTER TABLE ONLY public.canonical_stems
    ADD CONSTRAINT canonical_stems_pkey PRIMARY KEY (id);


--
-- Name: canonicals canonicals_pkey; Type: CONSTRAINT; Schema: public; Owner: dimus
--

ALTER TABLE ONLY public.canonicals
    ADD CONSTRAINT canonicals_pkey PRIMARY KEY (id);


--
-- Name: data_sources data_sources_pkey; Type: CONSTRAINT; Schema: public; Owner: dimus
--

ALTER TABLE ONLY public.data_sources
    ADD CONSTRAINT data_sources_pkey PRIMARY KEY (id);


--
-- Name: name_string_indices name_string_indices_pkey; Type: CONSTRAINT; Schema: public; Owner: dimus
--

ALTER TABLE ONLY public.name_string_indices
    ADD CONSTRAINT name_string_indices_pkey PRIMARY KEY (data_source_id, record_id, name_string_id);


--
-- Name: name_strings name_strings_pkey; Type: CONSTRAINT; Schema: public; Owner: dimus
--

ALTER TABLE ONLY public.name_strings
    ADD CONSTRAINT name_strings_pkey PRIMARY KEY (id);


--
-- Name: vernacular_string_indices vernacular_string_indices_pkey; Type: CONSTRAINT; Schema: public; Owner: dimus
--

ALTER TABLE ONLY public.vernacular_string_indices
    ADD CONSTRAINT vernacular_string_indices_pkey PRIMARY KEY (data_source_id, record_id, vernacular_string_id);


--
-- Name: vernacular_strings vernacular_strings_pkey; Type: CONSTRAINT; Schema: public; Owner: dimus
--

ALTER TABLE ONLY public.vernacular_strings
    ADD CONSTRAINT vernacular_strings_pkey PRIMARY KEY (id);


--
-- Name: word_name_strings word_name_strings_pkey; Type: CONSTRAINT; Schema: public; Owner: dimus
--

ALTER TABLE ONLY public.word_name_strings
    ADD CONSTRAINT word_name_strings_pkey PRIMARY KEY (word_id, name_string_id);


--
-- Name: words words_pkey; Type: CONSTRAINT; Schema: public; Owner: dimus
--

ALTER TABLE ONLY public.words
    ADD CONSTRAINT words_pkey PRIMARY KEY (id, normalized);


--
-- Name: accepted_record_id; Type: INDEX; Schema: public; Owner: dimus
--

CREATE INDEX accepted_record_id ON public.name_string_indices USING btree (accepted_record_id);


--
-- Name: canonical; Type: INDEX; Schema: public; Owner: dimus
--

CREATE INDEX canonical ON public.name_strings USING btree (canonical_id);


--
-- Name: canonical_full; Type: INDEX; Schema: public; Owner: dimus
--

CREATE INDEX canonical_full ON public.name_strings USING btree (canonical_full_id);


--
-- Name: canonical_stem; Type: INDEX; Schema: public; Owner: dimus
--

CREATE INDEX canonical_stem ON public.name_strings USING btree (canonical_stem_id);


--
-- Name: lang_code; Type: INDEX; Schema: public; Owner: dimus
--

CREATE INDEX lang_code ON public.vernacular_string_indices USING btree (lang_code);


--
-- Name: name_string_id; Type: INDEX; Schema: public; Owner: dimus
--

CREATE INDEX name_string_id ON public.name_string_indices USING btree (name_string_id);


--
-- Name: verification_canonical_id_idx; Type: INDEX; Schema: public; Owner: dimus
--

CREATE INDEX verification_canonical_id_idx ON public.verification USING btree (canonical_id);


--
-- Name: verification_name_string_id_idx; Type: INDEX; Schema: public; Owner: dimus
--

CREATE INDEX verification_name_string_id_idx ON public.verification USING btree (name_string_id);


--
-- Name: verification_year_idx; Type: INDEX; Schema: public; Owner: dimus
--

CREATE INDEX verification_year_idx ON public.verification USING btree (year);


--
-- Name: vernacular_string_id; Type: INDEX; Schema: public; Owner: dimus
--

CREATE INDEX vernacular_string_id ON public.vernacular_string_indices USING btree (vernacular_string_id);


--
-- Name: words_modified; Type: INDEX; Schema: public; Owner: dimus
--

CREATE INDEX words_modified ON public.words USING btree (modified);


--
-- Name: SCHEMA public; Type: ACL; Schema: -; Owner: dimus
--

REVOKE USAGE ON SCHEMA public FROM PUBLIC;
GRANT ALL ON SCHEMA public TO postgres;


--
-- PostgreSQL database dump complete
--

