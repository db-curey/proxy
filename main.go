package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/gofiber/fiber/v3"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"log"
	"math"
	"strconv"
	"strings"
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
	app.Post("/tx/begin", beginTxHandler)
	app.Post("/tx/commit", finishTxHandler)
	app.Post("/tx/rollback", finishTxHandler)
	app.Post("/query", queryHandler)
	app.Post("/exec", execHandler)

	// Start the server on port 3000
	log.Fatal(app.Listen(":3000"))
}

var (
	bgCtx, bgCancel = context.WithCancel(context.Background())
	clients         = make([]*Client, math.MaxUint16)
	clientSeq       = new(uint32)
)

type Client struct {
	transactions   []pgx.Tx
	token          string
	conn           *pgx.Conn
	expire         *time.Timer
	id             uint32
	transactionSeq uint32
}

func connectHandler(ctx fiber.Ctx) error {
	queries := ctx.Queries()
	if queries["port"] != "" {
		queries["port"] = "5432"
	}
	conn, err := pgx.Connect(bgCtx, "postgres://"+queries["user"]+":"+queries["password"]+"@"+queries["host"]+":"+queries["port"]+"/"+queries["dbname"])
	if err != nil {
		return ctx.Status(fiber.StatusInternalServerError).JSON(map[string]string{"error": err.Error()})
	}
	c := &Client{
		conn:         conn,
		token:        uuid.New().String(),
		id:           atomic.AddUint32(clientSeq, 1),
		transactions: make([]pgx.Tx, math.MaxUint16),
	}
	for {
		if clients[c.id%math.MaxUint16] != nil {
			c.id = atomic.AddUint32(clientSeq, 1)
		} else {
			clients[c.id%math.MaxUint16] = c
			break
		}
	}

	go func() {
		c.expire = time.NewTimer(time.Second * 3600)
		<-c.expire.C
		c.expire.Stop()
		if clients[c.id%math.MaxUint16] == c {
			clients[c.id%math.MaxUint16] = nil
		}
	}()

	return ctx.Status(fiber.StatusOK).JSON(map[string]any{"token": c.token, "connectionId": c.id})
}

func getClient(req DbRequest) *Client {
	cli := clients[req.ConnectionId%math.MaxUint16]
	if cli != nil && cli.token == req.Token {
		cli.expire.Reset(time.Second * 120)
		return cli
	}
	return nil
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

func beginTxHandler(ctx fiber.Ctx) error {
	req := DbRequest{}
	err := json.Unmarshal(ctx.Body(), &req)
	if err != nil {
		return ctx.Status(fiber.StatusBadRequest).JSON(map[string]string{"error": err.Error()})
	}
	cli := getClient(req)
	if cli != nil {
		id, err := cli.BeginTx()
		if err != nil {
			return ctx.Status(fiber.StatusInternalServerError).JSON(map[string]string{"error": err.Error()})
		}
		return ctx.Status(fiber.StatusOK).JSON(map[string]uint32{"transaction_id": id})
	}
	return ctx.Status(fiber.StatusUnauthorized).JSON(map[string]string{"error": "Invalid token"})
}

func finishTxHandler(ctx fiber.Ctx) error {
	req := DbRequest{}
	err := json.Unmarshal(ctx.Body(), &req)
	if err != nil {
		return ctx.Status(fiber.StatusBadRequest).JSON(map[string]string{"error": err.Error()})
	}
	cli := getClient(req)
	if cli != nil {
		if req.TransactionID != "" {
			txId, _ := strconv.ParseUint(req.TransactionID, 10, 64)
			tx := cli.transactions[txId%math.MaxUint16]
			if tx != nil {
				var err error
				if strings.Contains(ctx.BaseURL(), "/tx/rollback") {
					err = tx.Rollback(ctx.Context())
				} else if strings.Contains(ctx.BaseURL(), "/tx/commit") {
					err = tx.Commit(ctx.Context())
				}
				if err != nil {
					return ctx.Status(fiber.StatusInternalServerError).JSON(map[string]string{"error": err.Error()})
				}
				return ctx.Status(fiber.StatusOK).JSON(map[string]bool{"ok": true})
			}
		}
	}
	return ctx.Status(fiber.StatusUnauthorized).JSON(map[string]string{"error": "Invalid token"})
}

func (cli *Client) BeginTx() (id uint32, err error) {
	ctx, _ := context.WithTimeout(bgCtx, time.Second*60)
	tx, err := cli.conn.Begin(ctx)
	if err != nil {
		return 0, err
	}
	id = atomic.AddUint32(&cli.transactionSeq, 1)
	cli.transactions[id%math.MaxUint16] = tx
	return id, nil
}

type ExecRequest struct {
	DbRequest
	SQLs []string `json:"sqls"`
	Args [][]any  `json:"args"`
}

func execHandler(ctx fiber.Ctx) error {
	req := ExecRequest{}
	err := json.Unmarshal(ctx.Body(), &req)
	if err != nil {
		return ctx.Status(fiber.StatusBadRequest).JSON(map[string]string{"error": err.Error()})
	}
	cli := getClient(req.DbRequest)
	if cli != nil {
		if req.TransactionID != "" {
			txId, _ := strconv.ParseUint(req.TransactionID, 10, 64)
			tx := cli.transactions[txId%math.MaxUint16]
			defer func() {
				cli.transactions[txId%math.MaxUint16] = nil
			}()
			if tx != nil {
				statusCode, data := exec(ctx.Context(), tx, req)
				return ctx.Status(statusCode).JSON(data)
			}
		}
		statusCode, data := exec(ctx.Context(), cli.conn, req)
		return ctx.Status(statusCode).JSON(data)
	} else {
		return ctx.Status(fiber.StatusUnauthorized).JSON(map[string]string{"error": "Invalid token"})
	}
}

func queryHandler(ctx fiber.Ctx) error {
	req := QueryRequest{}
	err := json.Unmarshal(ctx.Body(), &req)
	if err != nil {
		return ctx.Status(fiber.StatusBadRequest).JSON(map[string]string{"error": err.Error()})
	}
	cli := getClient(req.DbRequest)
	if cli != nil {
		if req.TransactionID != "" {
			txId, _ := strconv.ParseUint(req.TransactionID, 10, 64)
			tx := cli.transactions[txId%math.MaxUint16]
			if tx != nil {
				statusCode, data := query(ctx.Context(), tx, req)
				return ctx.Status(statusCode).JSON(data)
			}
		}
		statusCode, data := query(ctx.Context(), cli.conn, req)
		_, err = ctx.Status(statusCode).Write(data)
		return err
	} else {
		return ctx.Status(fiber.StatusUnauthorized).JSON(map[string]string{"error": "Invalid token"})
	}
}

type Queryable interface {
	Prepare(ctx context.Context, name, sql string) (*pgconn.StatementDescription, error)
	Exec(ctx context.Context, sql string, arguments ...any) (commandTag pgconn.CommandTag, err error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

type ExecResult struct {
	RowsAffected int64  `json:"rows_affected"`
	Error        string `json:"error"`
}

func exec(ctx context.Context, cli Queryable, req ExecRequest) (status int, results []ExecResult) {
	results = make([]ExecResult, len(req.SQLs))
	for i, sql := range req.SQLs {
		tag, err := cli.Exec(ctx, sql, req.Args[i]...)
		if err != nil {
			results[i].Error = err.Error()
		}
		if !tag.Select() {
			results[i].RowsAffected = tag.RowsAffected()
		}
	}
	return fiber.StatusOK, results
}

func query(ctx context.Context, cli Queryable, req QueryRequest) (status int, data []byte) {
	rows, err := cli.Query(ctx, req.SQL, req.Args...)
	if err != nil {
		data, _ = json.Marshal(map[string]string{"error": err.Error()})
		return fiber.StatusInternalServerError, data
	}
	defer rows.Close()
	data, err = makeRawOutput(rows)
	if err != nil {
		fmt.Println(err)
		return fiber.StatusInternalServerError, data
	}
	return fiber.StatusOK, data
}

func makeRawOutput(rows pgx.Rows) (data []byte, err error) {
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
		nilCheck            = make([]bool, l)
	)
	_ = binary.Write(resBuff, binary.LittleEndian, l)
	for i, col := range cols {
		isColumnLengthFixed[i], columnsDataLen[i] = isFixedLengthColumnType(col)
		_ = binary.Write(resBuff, binary.LittleEndian, columnsDataLen[i])
		_ = binary.Write(resBuff, binary.LittleEndian, uint16(col.DataTypeOID))
	}
	for rows.Next() {
		columnData = rows.RawValues()
		rowsCnt++
		nilCheck = make([]bool, l)
		for i = 0; i < l; i++ {
			// if i is fixed length write to stack
			// else write to heap and write pointer to stack
			if columnData[i] == nil {
				nilCheck[i] = true
				resBuff.Write(make([]byte, columnsDataLen[i]))
			} else if isColumnLengthFixed[i] {
				_, _ = resBuff.Write(columnData[i]) // stack is already filled with fixed length columns
			} else {
				thisHeapLen = uint32(len(columnData[i]))
				_ = binary.Write(resBuff, binary.LittleEndian, heapBuffLen)
				_ = binary.Write(resBuff, binary.LittleEndian, thisHeapLen)
				heapBuffLen += thisHeapLen
				_, _ = heapBuff.Write(columnData[i])
			}
		}
		_, _ = resBuff.Write(boolsToBytes(nilCheck))

	}
	_, _ = heapBuff.WriteTo(resBuff)
	resData := resBuff.Bytes()
	binary.LittleEndian.PutUint32(resData[0:4], uint32(len(resData)))
	binary.LittleEndian.PutUint32(resData[4:8], rowsCnt)
	return resData, nil
}

func boolsToBytes(t []bool) []byte {
	b := make([]byte, (len(t)+7)/8)
	for i, x := range t {
		if x {
			b[i/8] |= 0x80 >> uint(i%8)
		}
	}
	return b
}

func isFixedLengthColumnType(col pgconn.FieldDescription) (isFixedLenCol bool, fixLen uint16) {
	if col.DataTypeSize == -1 {
		return false, 8
	}
	return true, uint16(col.DataTypeSize)
}
