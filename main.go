package main

import (
	"bytes"
	"encoding/json"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"log"
	"math"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/gofiber/fiber/v3"
)

func main() {
	// Initialize a new Fiber app
	app := fiber.New()

	// Define a route for the GET method on the root path '/'
	app.Get("/connect", connect)

	// Start the server on port 3000
	log.Fatal(app.Listen(":3000"))
}

var (
	clients   = make([]*Client, math.MaxUint16)
	clientSeq = new(uint32)
)

type Client struct {
	config *pgx.ConnConfig
	conn   *pgx.Conn
	id     uint32
	token  string
	expire *time.Timer
}

func connect(ctx fiber.Ctx) error {
	queries := ctx.Queries()
	config := &pgx.ConnConfig{}
	config.Host, config.User, config.Password, config.Database = queries["host"], queries["user"], queries["password"], queries["dbname"]
	if config.Host == "" || config.User == "" || config.Password == "" || config.Database == "" {
		return ctx.Status(fiber.StatusBadRequest).JSON(map[string]string{"error": "Missing parameters"})
	}
	if queries["port"] != "" {
		p, _ := strconv.ParseUint(queries["port"], 10, 16)
		config.Port = uint16(p)
	} else {
		config.Port = 5432
	}
	conn, err := pgx.ConnectConfig(ctx.Context(), config)
	if err != nil {
		return ctx.Status(fiber.StatusInternalServerError).JSON(map[string]string{"error": err.Error()})
	}
	c := &Client{
		config: config,
		conn:   conn,
		token:  uuid.New().String(),
		id:     atomic.AddUint32(clientSeq, 1),
	}
	for {
		if clients[c.id%math.MaxUint16] != nil {
			c.id = atomic.AddUint32(clientSeq, 1)
		} else {
			clients[c.id%math.MaxUint16] = c
			break
		}
	}

	c.expire = time.NewTimer(time.Second * 120)

	return ctx.Status(fiber.StatusOK).JSON(map[string]any{"token": c.token, "connectionId": c.id})
}

type DbRequest struct {
	Token         string `json:"token"`
	ConnectionId  uint32 `json:"connection_id"`
	TransactionID string `json:"transaction_id"`
}

type QueryRequest struct {
	DbRequest
	SQL  string   `json:"sql"`
	Args []string `json:"args"`
}

func query(ctx fiber.Ctx) error {
	req := QueryRequest{}
	err := json.Unmarshal(ctx.Body(), &req)
	if err != nil {
		return ctx.Status(fiber.StatusBadRequest).JSON(map[string]string{"error": err.Error()})
	}
	cli := clients[req.ConnectionId%math.MaxUint16]
	if cli != nil && cli.token == req.Token {
		rows, err := cli.conn.Query(ctx.Context(), req.SQL, req.Args)
		if err != nil {
			return ctx.Status(fiber.StatusInternalServerError).JSON(map[string]string{"error": err.Error()})
		}
		defer rows.Close()
		data, columnLengths := makeOutput(rows)
	} else {
		return ctx.Status(fiber.StatusUnauthorized).JSON(map[string]string{"error": "Invalid token"})
	}
	return nil
}

func makeOutput(rows pgx.Rows) (data []byte, columnLengths uint16) {
	rowsValue := rows.RawValues()
	cols := rows.FieldDescriptions()
	for _, col := range cols {
		// TODO: get fix length columns
	}
	columnLengths = uint16(len(rowsValue[0]))
	if len(rowsValue) == 0 || columnLengths == 0 {
		return []byte{}, 0
	}
	stackBuff, heapBuff := bytes.NewBuffer(nil), bytes.NewBuffer(nil)
	var i uint16
	for _, columns := range rowsValue {
		for i = 0; i < columnLengths; i++ {
			// if i is fixed length write to stack
			// else write to heap and write pointer to stack
			stackBuff.WriteByte(columns[i])
		}
	}
}
