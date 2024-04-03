package database

import (
	"context"
	"database/sql"
	"fmt"
	"l0/models"
)

const (
	host     = "localhost"
	port     = 5432
	user     = "habrpguser"
	password = "pgpwd4habr"
	dbname   = "habrdb"
)

type Database struct {
	db *sql.DB
}

func (database *Database) Connect() error {
	psqlconn := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", host, port, user, password, dbname)

	// open database
	db, err := sql.Open("postgres", psqlconn)
	if err != nil {
		return err
	}

	// check db
	err = db.Ping()
	if err != nil {
		return err
	}

	fmt.Println("Connected!")

	database.db = db
	return nil
}

func (database *Database) Disconnect() {
	database.db.Close()
}

func (database *Database) GetIdList() (ids []int, err error) {
	stmt := `select id from "L0_schema"."tbl_orders"`
	rows, err := database.db.Query(stmt)

	if err != nil {
		return
	}

	for rows.Next() {
		var id sql.NullInt32
		if err = rows.Scan(&id); err != nil {
			return
		}
		ids = append(ids, int(id.Int32))
	}

	return
}

func (database *Database) GetDataById(id int) (order models.Order, err error) {

	selectOrderStmt := `
			select o.order_uid, o.track_number, o.entry, o.locale, o.internal_signature, o.customer_id, o.delivery_service, o.shardkey, o.sm_id, o.date_created, o.oof_shard,
				   cl.name, cl.phone, cl.email, a.zip, a.city, a.address, a. region,
				   p.transaction, p.request_id, p.currency, p.provider, p.amount, p.payment_dt, p.bank, p.delivery_cost, p.goods_total, p.custom_fee
			from "L0_schema"."tbl_orders" o
			inner join "L0_schema"."tbl_payments" p on o.payment_id = o.id
			inner join "L0_schema"."tbl_clients_addresses" ca on o.delivery_id = ca.id
			inner join "L0_schema"."tbl_clients" cl on ca.client_id = cl.id
			inner join "L0_schema"."tbl_addresses" a on ca.address_id = a.id
			where o.id = $1`
	data, err := database.db.Query(selectOrderStmt, id)

	if err != nil {
		return
	}
	if data.Next() {
		err = data.Scan(&order.OrderUid,
			&order.TrackNumber,
			&order.Entry,
			&order.Locale,
			&order.InternalSignature,
			&order.CustomerId,
			&order.DeliveryService,
			&order.Shardkey,
			&order.SmId,
			&order.DateCreated,
			&order.OofShard,
			&order.Delivery.Name,
			&order.Delivery.Phone,
			&order.Delivery.Email,
			&order.Delivery.Zip,
			&order.Delivery.City,
			&order.Delivery.Address,
			&order.Delivery.Region,
			&order.Payment.Transaction,
			&order.Payment.RequestId,
			&order.Payment.Currency,
			&order.Payment.Provider,
			&order.Payment.Amount,
			&order.Payment.PaymentDt,
			&order.Payment.Bank,
			&order.Payment.DeliveryCost,
			&order.Payment.GoodsTotal,
			&order.Payment.CustomFee)
		if err != nil {
			return
		}
	} else {
		return models.Order{}, nil
	}

	selectItemsStmt := `
			select i.chrt_id, i.track_number, i.price, i.rid, i.name, i.sale, i.size, i.total_price, i.nm_id, i.brand, i.status
			from "L0_schema"."tbl_order_items" oi
			inner join "L0_schema"."tbl_items" i on oi.item_id = i.id
			where oi.order_id = $1`

	rows, err := database.db.Query(selectItemsStmt, id)
	if err != nil {
		return
	}

	items := []models.Item{}
	for rows.Next() {
		item := models.Item{}
		err = rows.Scan(&item.ChrtId,
			&item.TrackNumber,
			&item.Price,
			&item.Rid,
			&item.Name,
			&item.Sale,
			&item.Size,
			&item.TotalPrice,
			&item.NmId,
			&item.Brand,
			&item.Status)
		if err != nil {
			return
		}
		items = append(items, item)
	}

	order.Items = items
	return
}

func (database *Database) Insert(order models.Order) error {
	// connection string

	var row sql.NullInt32
	ctx := context.Background()
	tx, err := database.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	insertClientStmt := `insert into "L0_schema"."tbl_clients"("name", "phone", "email") values($1, $2, $3) ON CONFLICT (phone) do update set name =$1, email = $3 returning id`
	err = tx.QueryRow(insertClientStmt, order.Delivery.Name, order.Delivery.Phone, order.Delivery.Email).Scan(&row)
	if err != nil {
		return err
	}

	clientId := row.Int32

	insertAddressStmt := `insert into "L0_schema"."tbl_addresses"("zip", "city", "address","region") values($1, $2, $3, $4) ON CONFLICT (city, address) do update set zip = $1, region = $4 returning id`
	err = tx.QueryRow(insertAddressStmt, order.Delivery.Zip, order.Delivery.City, order.Delivery.Address, order.Delivery.Region).Scan(&row)
	if err != nil {
		return err
	}

	addressId := row.Int32

	insertClientAddrStmt := `insert into "L0_schema"."tbl_clients_addresses"("client_id", "address_id") values($1, $2) ON CONFLICT (client_id, address_id) do update set client_id = $1 returning id`
	err = tx.QueryRow(insertClientAddrStmt, clientId, addressId).Scan(&row)
	if err != nil {
		return err
	}

	clientAddrId := row.Int32

	insertPaymentStmt := `insert into "L0_schema"."tbl_payments"("transaction", 
	                                                 "request_id", 
													 "currency",
													 "provider",
													 "amount",
													 "payment_dt",
													 "bank",
													 "delivery_cost",
													 "goods_total",
													 "custom_fee") 
	                      values($1, $2, $3, $4, $5, $6, $7, $8, $9, $10) ON CONFLICT (transaction) 
						  do update set  request_id = $2,
						                 currency = $3,
										 provider = $4,
										 amount = $5,
										 payment_dt = $6,
										 bank = $7,
										 delivery_cost = $8,
										 goods_total = $9,
										 custom_fee = $10
						  returning id`
	err = tx.QueryRow(insertPaymentStmt, order.Payment.Transaction,
		order.Payment.RequestId,
		order.Payment.Currency,
		order.Payment.Provider,
		order.Payment.Amount,
		order.Payment.PaymentDt,
		order.Payment.Bank,
		order.Payment.DeliveryCost,
		order.Payment.GoodsTotal,
		order.Payment.CustomFee).Scan(&row)
	if err != nil {
		return err
	}

	paymentId := row.Int32

	ids := make([]int, 0, len(order.Items))
	insertItemStmt := ""
	for _, item := range order.Items {
		insertItemStmt = `insert into "L0_schema"."tbl_items"("chrt_id", 
		                                          "track_number", 
												  "price",
												  "rid",
												  "name",
												  "sale",
												  "size",
												  "total_price",
												  "nm_id",
												  "brand",
												  "status") 
						  values($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11) ON CONFLICT (chrt_id) 
						  do update set track_number = $2,
						                price = $3,
										rid = $4,
										name = $5,
										sale = $6,
										size = $7,
										total_price = $8,
										nm_id = $9,
										brand = $10,
										status = $11
						  returning id`
		err = tx.QueryRow(insertItemStmt, item.ChrtId,
			item.TrackNumber,
			item.Price,
			item.Rid,
			item.Name,
			item.Sale,
			item.Size,
			item.TotalPrice,
			item.NmId,
			item.Brand,
			item.Status).Scan(&row)
		if err != nil {
			return err
		}
		ids = append(ids, int(row.Int32))
	}

	insertOrderStmt := `insert into "L0_schema"."tbl_orders"("order_uid",
	                                             "track_number",
												 "entry",
												 "delivery_id",
												 "payment_id",
												 "locale",
												 "internal_signature",
												 "customer_id",
												 "delivery_service",
												 "shardkey",
												 "sm_id",
												 "date_created",
												 "oof_shard") 
	                      values($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13) ON CONFLICT (order_uid) 
						  do update set track_number = $2,
						                entry = $3,
										delivery_id = $4,
										payment_id = $5,
										locale = $6,
										internal_signature = $7,
										customer_id = $8,
										delivery_service = $9,
										shardkey = $10,
										sm_id = $11,
										date_created = $12,
										oof_shard = $13
						  returning id`
	err = tx.QueryRow(insertOrderStmt, order.OrderUid,
		order.TrackNumber,
		order.Entry,
		clientAddrId,
		paymentId,
		order.Locale,
		order.InternalSignature,
		order.CustomerId,
		order.DeliveryService,
		order.Shardkey,
		order.SmId,
		order.DateCreated,
		order.OofShard).Scan(&row)
	if err != nil {
		return err
	}

	orderId := row.Int32

	for _, item := range ids {
		insertOrderItemsStmt := `insert into "L0_schema"."tbl_order_items"("order_id", "item_id") values($1, $2) ON CONFLICT (order_id, item_id) do update set order_id = $1`
		_, err = tx.Exec(insertOrderItemsStmt, orderId, item)
		if err != nil {
			return err
		}
	}

	if err = tx.Commit(); err != nil {
		return err
	}

	return nil
}
