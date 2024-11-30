package main

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

/*
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
*/

func TestQuery(t *testing.T) {
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, "postgres://app:app@localhost:5432/postgres?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(ctx)
	client := &Client{nil, conn, 1, "123", time.NewTimer(time.Second * 120)}
	status, data := query(ctx, client, QueryRequest{SQL: "SELECT * FROM public.test where i64 = $1", Args: []any{int64(1)}})
	assert.Equal(t, 200, status)
	assert.Greater(t, len(data), 0)
	fmt.Println(string(data))
}
