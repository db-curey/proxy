package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"github.com/gofiber/fiber/v3"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"log"
	"math"
	"strconv"
	"sync/atomic"
	"time"

	_ "github.com/gofiber/fiber/v3"
	_ "github.com/jackc/pgx/v5"
)

func main() {
	// Initialize a new Fiber app
	app := fiber.New()

	// Define a route for the GET method on the root path '/'
	app.Get("/connect", connectHandler)
	app.Post("/query", queryHandler)

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

func connectHandler(ctx fiber.Ctx) error {
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
	SQL  string `json:"sql"`
	Args []any  `json:"args"`
}

func queryHandler(ctx fiber.Ctx) error {
	req := QueryRequest{}
	err := json.Unmarshal(ctx.Body(), &req)
	if err != nil {
		return ctx.Status(fiber.StatusBadRequest).JSON(map[string]string{"error": err.Error()})
	}
	cli := clients[req.ConnectionId%math.MaxUint16]
	if cli != nil && cli.token == req.Token {
		statusCode, data := query(ctx.Context(), cli, req)
		return ctx.Status(statusCode).JSON(data)
	} else {
		return ctx.Status(fiber.StatusUnauthorized).JSON(map[string]string{"error": "Invalid token"})
	}
}

func query(ctx context.Context, cli *Client, req QueryRequest) (status int, data []byte) {
	rows, err := cli.conn.Query(ctx, req.SQL, req.Args...)
	if err != nil {
		data, _ = json.Marshal(map[string]string{"error": err.Error()})
		return fiber.StatusInternalServerError, data
	}
	defer rows.Close()
	return fiber.StatusOK, makeOutput(rows)
}

func makeOutput(rows pgx.Rows) (data []byte) {
	a := rows.Next()
	_ = a
	rowsValue := rows.RawValues()
	cols := rows.FieldDescriptions()
	var (
		l                   = uint16(len(cols))
		isColumnLengthFixed = make([]bool, l)
		columnsDataLen      = make([]uint16, l)
		resBuff             = bytes.NewBuffer([]byte{0, 0, 0, 0, 0, 0, 0, 0}) //4 bytes for ttl len, 4 bytes for rows count
		heapBuff            = bytes.NewBuffer(nil)
		i                   uint16
		columnData          [][]byte
		heapBuffLen         = uint32(0)
		thisHeapLen         = uint32(0)
		rowsCnt             = uint32(0)
	)
	_ = binary.Write(resBuff, binary.LittleEndian, l)
	for i, col := range cols {
		isColumnLengthFixed[i], columnsDataLen[i] = isFixedLengthColumnType(col)
		binary.LittleEndian.AppendUint16(resBuff.Bytes(), columnsDataLen[i])
		binary.LittleEndian.AppendUint32(resBuff.Bytes(), col.DataTypeOID)
	}
	if len(rowsValue) == 0 || l == 0 {
		return []byte{}
	}
	for rows.Next() {
		columnData = rows.RawValues()
		rowsCnt++
		for i = 0; i < l; i++ {
			// if i is fixed length write to stack
			// else write to heap and write pointer to stack
			if isColumnLengthFixed[i] {
				_, _ = resBuff.Write(columnData[i]) // stack is already filled with fixed length columns
			} else {
				thisHeapLen = uint32(heapBuff.Len())
				_ = binary.Write(resBuff, binary.LittleEndian, heapBuffLen)
				_ = binary.Write(resBuff, binary.LittleEndian, thisHeapLen)
				heapBuffLen += thisHeapLen
				_, _ = heapBuff.Write(columnData[i])
			}
		}
	}
	_, _ = heapBuff.WriteTo(resBuff)
	resData := resBuff.Bytes()
	binary.LittleEndian.PutUint32(resData[0:4], uint32(len(resData)))
	binary.LittleEndian.PutUint32(resData[4:8], rowsCnt)
	return resData
}

func isFixedLengthColumnType(col pgconn.FieldDescription) (isFixedLenCol bool, fixLen uint16) {
	if col.DataTypeSize == -1 {
		return false, 8
	}
	return true, uint16(col.DataTypeSize)
}
