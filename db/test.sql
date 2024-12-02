CREATE TABLE public.test (
                             id uuid NOT NULL,
                             i64 int8 NOT NULL,
                             i64_null int8 NULL,
                             "varchar" varchar(255) NOT NULL,
                             "text" text NOT NULL,
                             "jsonb" jsonb DEFAULT '{}'::json NOT NULL,
                             "bool" bool NOT NULL,
                             timestampz timestamptz DEFAULT now() NOT NULL,
                             bytes bytea NOT NULL,
                             i32_list _int4 DEFAULT '{}'::integer[] NOT NULL,
                             "decimal" numeric(15, 5) NOT NULL,
                             CONSTRAINT newtable_pk PRIMARY KEY (id)
);

INSERT INTO public.test
(id, i64, i64_null, "varchar", "text", "jsonb", "bool", timestampz, bytes, i32_list, "decimal")
VALUES(uuid_in(md5(random()::text || random()::text)::cstring), 1, 0, 'hi', 'bye', '{"a":"b"}'::json, false, now(), '{1,2}'::bytea, '{1,2,3}'::integer[], 1.2345);

INSERT INTO public.test
(id, i64, i64_null, "varchar", "text", "jsonb", "bool", timestampz, bytes, i32_list, "decimal")
VALUES(uuid_in(md5(random()::text || random()::text)::cstring), 1, null, 'hihi', 'byebye', '{"c":"d"}'::json, false, now(), '{1,2,3}'::bytea, '{3,2,1}'::integer[], 2.3456);