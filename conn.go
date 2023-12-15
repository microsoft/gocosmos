package gocosmos

import (
	"context"
	"database/sql/driver"
	"errors"
	"time"
)

var (
	locGmt, _ = time.LoadLocation("GMT")
)

// Conn is Azure Cosmos DB implementation of driver.Conn.
type Conn struct {
	restClient *RestClient // Azure Cosmos DB REST API client.
	defaultDb  string      // default database used in Cosmos DB operations.
}

// Prepare implements driver.Conn/Prepare.
func (c *Conn) Prepare(query string) (driver.Stmt, error) {
	return c.PrepareContext(context.Background(), query)
}

// PrepareContext implements driver.ConnPrepareContext/PrepareContext.
//
// @Available since v0.2.1
func (c *Conn) PrepareContext(_ context.Context, query string) (driver.Stmt, error) {
	return parseQueryWithDefaultDb(c, c.defaultDb, query)
}

// Close implements driver.Conn/Close.
func (c *Conn) Close() error {
	return nil
}

// Begin implements driver.Conn/Begin.
func (c *Conn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

// BeginTx implements driver.ConnBeginTx/BeginTx.
//
// @Available since v0.2.1
func (c *Conn) BeginTx(_ context.Context, _ driver.TxOptions) (driver.Tx, error) {
	return nil, errors.New("transaction is not supported")
}

// CheckNamedValue implements driver.NamedValueChecker/CheckNamedValue.
func (c *Conn) CheckNamedValue(_ *driver.NamedValue) error {
	// since Cosmos DB is document db, it accepts any value types
	return nil
}
