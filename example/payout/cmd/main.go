package main

import (
	"context"
	"log"

	"github.com/soyart/satch/datasource/satchmongo"
	"github.com/soyart/satch/example/payout"
	"go.mongodb.org/mongo-driver/bson"
)

func main() {
	ctx := context.Background()
	mg, err := satchmongo.NewMongoDB(ctx, satchmongo.MongoDBConfig{
		Hosts:    "localhost:47017",
		Admin:    "lineman-admin",
		Username: "test_user",
		Password: "test_password",
	})
	if err != nil {
		panic(err.Error())
	}

	db := "example-payout"

	collPayouts := mg.Collection(db, "payouts")
	collAccounts := mg.Collection(db, "accounts")
	collCustomers := mg.Collection(db, "customers")

	var payouts []payout.Payout
	err = collPayouts.Find(ctx, bson.M{}, &payouts)
	if err != nil {
		log.Println("failed to find input payouts")
		panic(err)
	}

	var accounts []payout.Account
	err = collAccounts.Find(ctx, bson.M{}, &accounts)
	if err != nil {
		log.Println("failed to find input accounts")
		panic(err)
	}

	var customers []payout.Customer
	err = collCustomers.Find(ctx, bson.M{}, &customers)
	if err != nil {
		log.Println("failed to find input accounts")
		panic(err)
	}

	changes, err := payout.ProcessPayout(payout.Inputs{
		Payouts:   payouts,
		Customers: customers,
		Accounts:  accounts,
	})
	if err != nil {
		log.Println("error processing payouts")
		panic(err)
	}

	for i := range changes.List {
		log.Println("change", i, changes.List[i])
	}
}
