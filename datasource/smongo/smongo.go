// smongo provides opinionated building blocks for safe MongoDB transactions in batch jobs
//
// It focuses heavily on transactions, so if your use cases do not concern transactions,
// you can just implement your own satch.DataSource without using this package

package smongo

import (
	"context"
	"fmt"
	"reflect"
	"strings"

	"github.com/pkg/errors"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type TxFunc func(ctx mongo.SessionContext) (interface{}, error)

type MongoDBConfig struct {
	Hosts    string `json:"hosts" yaml:"hosts"`
	Admin    string `json:"admin" yaml:"admin"`
	Username string `json:"username" yaml:"username"`
	Password string `json:"password" yaml:"password"`
}

type MongoDB struct {
	cli *mongo.Client
}

func NewClient(
	ctx context.Context,
	conf MongoDBConfig,
	opts ...*options.ClientOptions,
) (
	*MongoDB,
	error,
) {
	connOpts := options.
		Client().
		SetHosts(strings.Split(conf.Hosts, ",")).
		SetAuth(options.Credential{
			// TODO: auth login
			// AuthSource: conf.Admin,
			Username: conf.Username,
			Password: conf.Password,
		})

	cli, err := mongo.Connect(ctx, append(opts, connOpts)...)
	if err != nil {
		return nil, err
	}

	return &MongoDB{cli: cli}, nil
}

func (m *MongoDB) Unwrap() *mongo.Client {
	return m.cli
}

func (m *MongoDB) Collection(db, coll string) *Collection {
	return &Collection{coll: m.cli.Database(db).Collection(coll)}
}

// DB-level transaction
func WithTxDb(
	ctx context.Context,
	db *mongo.Database,
	tx TxFunc,
) (
	interface{},
	error,
) {
	sess, err := db.Client().StartSession()
	if err != nil {
		return nil, errors.Wrapf(err, "failed to start tx for database: %s", db.Name())
	}

	defer func() {
		if err != nil {
			sess.AbortTransaction(ctx)
		}

		sess.EndSession(ctx)
	}()

	result, err := sess.WithTransaction(ctx, tx)
	if err != nil {
		return result, errors.Wrap(err, "failed to perform tx")
	}

	err = sess.CommitTransaction(ctx)
	if err != nil {
		return result, errors.Wrap(err, "failed to commit tx")
	}

	return result, nil
}

// DB-level transaction for a single collection
func WithTxColl(
	ctx context.Context,
	coll *mongo.Collection,
	tx TxFunc,
) (
	interface{},
	error,
) {
	sess, err := coll.Database().Client().StartSession(nil)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to start a new tx session in collection '%s'", coll.Name())
	}

	defer func() {
		if err != nil {
			sess.AbortTransaction(ctx)
		}

		sess.EndSession(ctx)
	}()

	result, err := sess.WithTransaction(ctx, tx)
	if err != nil {
		return result, errors.Wrap(err, "failed to perform tx")
	}

	err = sess.CommitTransaction(ctx)
	if err != nil {
		return result, errors.Wrap(err, "failed to commit tx")
	}

	return result, nil
}

func Find(
	ctx context.Context,
	coll *mongo.Collection,
	filter interface{},
	opts ...*options.FindOptions,
) (
	*mongo.Cursor,
	error,
) {
	tx := TxFind(coll, filter, opts...)
	resultTx, err := WithTxColl(ctx, coll, tx)
	if err != nil {
		return nil, err
	}

	result, ok := resultTx.(*mongo.Cursor)
	if !ok {
		return nil, fmt.Errorf("unexpected result type: %s", reflect.TypeOf(resultTx).String())
	}

	return result, nil
}

func BulkWrite(
	ctx context.Context,
	coll *mongo.Collection,
	writes []mongo.WriteModel,
	opts ...*options.BulkWriteOptions,
) (
	*mongo.BulkWriteResult,
	error,
) {
	tx := TxBulkWrite(coll, writes, opts...)
	resultTx, err := WithTxColl(ctx, coll, tx)
	if err != nil {
		return nil, err
	}

	result, ok := resultTx.(*mongo.BulkWriteResult)
	if !ok {
		return nil, fmt.Errorf("unexpected result type: %s", reflect.TypeOf(resultTx).String())
	}

	return result, nil
}

func BulkWriteColls(
	ctx context.Context,
	db *mongo.Database,
	collWrites map[string][]mongo.WriteModel,
	opts ...*options.BulkWriteOptions,
) (
	*mongo.BulkWriteResult,
	error,
) {
	tx := TxBulkWriteColls(db, collWrites, opts...)
	resultTx, err := WithTxDb(ctx, db, tx)
	if err != nil {
		return nil, err
	}

	result, ok := resultTx.(*mongo.BulkWriteResult)
	if !ok {
		return nil, fmt.Errorf("unexpected result type: %s", reflect.TypeOf(resultTx).String())
	}

	return result, nil
}

func InsertMany(
	ctx context.Context,
	coll *mongo.Collection,
	inserts []interface{},
	opts ...*options.InsertManyOptions,
) (
	*mongo.InsertManyResult,
	error,
) {
	tx := TxInsertMany(coll, inserts, opts...)
	resultTx, err := WithTxColl(ctx, coll, tx)
	if err != nil {
		return nil, err
	}

	result, ok := resultTx.(*mongo.InsertManyResult)
	if !ok {
		return nil, fmt.Errorf("unexpected result type: %s", reflect.TypeOf(resultTx).String())
	}

	return result, nil
}

func Update(
	ctx context.Context,
	coll *mongo.Collection,
	filter interface{},
	updates []interface{},
	opts ...*options.UpdateOptions,
) (
	*mongo.UpdateResult,
	error,
) {
	tx := TxUpdateMany(coll, filter, updates, opts...)
	resultTx, err := WithTxColl(ctx, coll, tx)
	if err != nil {
		return nil, err
	}

	result, ok := resultTx.(*mongo.UpdateResult)
	if !ok {
		return nil, fmt.Errorf("unexpected result type: %s", reflect.TypeOf(resultTx).String())
	}

	return result, nil
}

type Collection struct {
	coll *mongo.Collection
}

func NewCollection(client *mongo.Client, db, coll string) *Collection {
	return &Collection{coll: client.Database(db).Collection(coll)}
}

func (c *Collection) Unwrap() *mongo.Collection {
	return c.coll
}

func (c *Collection) Find(
	ctx context.Context,
	filter interface{},
	results interface{},
	opts ...*options.FindOptions,
) error {
	cursor, err := Find(ctx, c.coll, filter, opts...)
	if err != nil {
		return err
	}

	defer cursor.Close(ctx)

	return cursor.All(ctx, results)
}

func (c *Collection) BulkWrite(
	ctx context.Context,
	writes []mongo.WriteModel,
	opts ...*options.BulkWriteOptions,
) (
	*mongo.BulkWriteResult,
	error,
) {
	return BulkWrite(ctx, c.Unwrap(), writes, opts...)
}

func (c *Collection) InsertMany(
	ctx context.Context,
	inserts []interface{},
	opts ...*options.InsertManyOptions,
) (
	*mongo.InsertManyResult,
	error,
) {
	return InsertMany(ctx, c.Unwrap(), inserts, opts...)
}

func (c *Collection) Update(
	ctx context.Context,
	filter interface{},
	updates []interface{},
	opts ...*options.UpdateOptions,
) (
	*mongo.UpdateResult,
	error,
) {
	return Update(ctx, c.Unwrap(), filter, updates, opts...)
}
