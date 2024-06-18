package smongo

import (
	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Wraps a simple Mongo find inside a callback that can be sent to a MongoDB transaction
func TxFind(
	coll *mongo.Collection,
	filter interface{},
	opts ...*options.FindOptions,
) TxFunc {
	return func(ctx mongo.SessionContext) (interface{}, error) {
		// ctx will be replaced by caller to the active tx's context
		result, err := coll.Find(ctx, filter, opts...)
		if err != nil {
			logrus.Errorf("find: error finding with %v filter", filter)
		}

		return result, err
	}
}

// Wraps bulk writes inside a callback that can be sent to a MongoDB transaction
func TxBulkWrite(
	coll *mongo.Collection,
	writes []mongo.WriteModel,
	opts ...*options.BulkWriteOptions,
) TxFunc {
	return func(ctx mongo.SessionContext) (interface{}, error) {
		result, err := coll.BulkWrite(ctx, writes, opts...)
		if err != nil {
			logrus.Errorf("bulkWrite: error bulk writing %d write models", len(writes))
		}

		return result, err
	}
}

// Wraps bulk writes across multiple collections inside a callback that can be sent to a MongoDB transaction.
// The collWrites are represented as map of collection name to writes.
func TxBulkWriteColls(
	db *mongo.Database,
	collWrites map[string][]mongo.WriteModel,
	opts ...*options.BulkWriteOptions,
) TxFunc {
	return func(ctx mongo.SessionContext) (interface{}, error) {
		results := make(map[string]interface{})

		for coll, writes := range collWrites {
			result, err := db.Collection(coll).BulkWrite(ctx, writes, opts...)
			if err != nil {
				logrus.Errorf("bulkWrite: error bulk writing %d write models", len(writes))
				return results, err
			}

			results[coll] = result
		}

		return results, nil
	}
}

// Wraps inserts (slice of `bson.M`s or `bson.D`s) inside a callback than can be sent to a MongoDB transaction
func TxInsertMany(
	coll *mongo.Collection,
	inserts []interface{},
	opts ...*options.InsertManyOptions,
) TxFunc {
	return func(ctx mongo.SessionContext) (interface{}, error) {
		result, err := coll.InsertMany(ctx, inserts, opts...)
		if err != nil {
			logrus.Errorf("insertMany: error inserting %d documents", len(inserts))
		}

		return result, err
	}
}

// Wraps updateMany inside a callback that can be sent to a MongoDB transaction
func TxUpdateMany(
	coll *mongo.Collection,
	filter interface{},
	updates []interface{},
	opts ...*options.UpdateOptions,
) TxFunc {
	return func(ctx mongo.SessionContext) (interface{}, error) {
		result, err := coll.UpdateMany(ctx, filter, updates, opts...)
		if err != nil {
			logrus.Errorf("update: error updating %d documents", len(updates))
		}

		return result, err
	}
}

// Wraps deleteMany inside a callback that can be sent to a MongoDB transaction
func TxDeleteMany(
	coll *mongo.Collection,
	filter interface{},
	opts ...*options.DeleteOptions,
) TxFunc {
	return func(ctx mongo.SessionContext) (interface{}, error) {
		result, err := coll.DeleteMany(ctx, filter, opts...)
		if err != nil {
			logrus.Errorf("update: error deleting documents '%s'", err)
		}

		return result, err
	}
}
