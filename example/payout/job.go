package payout

import (
	"context"
	"errors"
	"fmt"
	"log"
	"reflect"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"

	"github.com/soyart/satch/datasource/satchmongo"
)

type Job struct {
	start time.Time
}

type Inputs struct {
	T         time.Time // Settlement date
	Payouts   []Payout
	Customers []Customer
	Accounts  []Account
}

// Maps collection name to writes
// More iterable compared to OutputsV1
type OutputsV2 map[string][]mongo.WriteModel

type OutputsV1 struct {
	Payouts   []mongo.WriteModel
	Customers []mongo.WriteModel
	Accounts  []mongo.WriteModel
}

func New() *Job {
	return &Job{start: time.Now()}
}

func (j *Job) ID() string {
	return fmt.Sprintf("job-payout-%s", j.start)
}

func (j *Job) Run(ctx context.Context, inputs interface{}) (interface{}, error) {
	inputsPayout, ok := inputs.(Inputs)
	if !ok {
		return nil, fmt.Errorf("unexpected inputs type: '%s'", reflect.TypeOf(inputs).String())
	}

	changes, err := ProcessPayout(inputsPayout)
	if err != nil {
		return nil, err
	}

	return changes.OutputsV2(j.start), err
}

const DB = "example-payout"

type dataSource struct {
	db *satchmongo.MongoDB
}

func (d *dataSource) Unwrap() *mongo.Client {
	return d.db.Unwrap()
}

func (d *dataSource) UnwrapSatch() *satchmongo.MongoDB {
	return d.db
}

func (d *dataSource) Collection(db, coll string) *mongo.Collection {
	return d.db.Collection(db, coll).Unwrap()
}

func (d *dataSource) CollectionSatch(db, coll string) *satchmongo.Collection {
	return d.db.Collection(db, coll)
}

func (d *dataSource) LockRead(_ context.Context) error {
	return nil
}

func (d *dataSource) LockWrite(_ context.Context) error {
	return errors.New("should not lock writes for this job")
}

func (d *dataSource) Inputs(ctx context.Context) (interface{}, error) {
	collPayouts := d.CollectionSatch(DB, "payouts")
	collAccounts := d.CollectionSatch(DB, "accounts")
	collCustomers := d.CollectionSatch(DB, "customers")

	var customers []Customer
	err := collCustomers.Find(ctx, bson.M{}, &customers)
	if err != nil {
		log.Println("failed to find input customers")
		return nil, err
	}

	var payouts []Payout
	err = collPayouts.Find(ctx, bson.M{}, &payouts)
	if err != nil {
		log.Println("failed to find input payouts")
		return nil, err
	}

	var accounts []Account
	err = collAccounts.Find(ctx, bson.M{}, &accounts)
	if err != nil {
		log.Println("failed to find input accounts")
		return nil, err
	}

	return Inputs{
		Payouts:   payouts,
		Customers: customers,
		Accounts:  accounts,
	}, nil
}

func (d *dataSource) Commit(ctx context.Context, data interface{}) error {
	outputs, ok := data.(OutputsV2)
	if !ok {
		return fmt.Errorf("unexpected data type: '%s'", reflect.TypeOf(data).String())
	}

	db := d.Unwrap().Database(DB)
	tx := satchmongo.TxBulkWriteColls(db, outputs)

	_, err := satchmongo.WithTxDb(ctx, db, tx)
	if err != nil {
		return err
	}

	return nil
}
