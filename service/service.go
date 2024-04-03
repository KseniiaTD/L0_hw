package service

import (
	"encoding/json"
	"html/template"
	"l0/cache"
	"l0/database"
	"l0/models"
	"log"
	"net/http"
	"strconv"
	"time"
)

var tpl_data = template.Must(template.ParseFiles("data.html"))
var tpl_data_not_found = template.Must(template.ParseFiles("data_not_found.html"))

type Service struct {
	db    database.Database
	cache *cache.Cache
}

func New(db database.Database, cache *cache.Cache) Service {
	return Service{
		db:    db,
		cache: cache,
	}
}

func (s *Service) GetByIdHandler(w http.ResponseWriter, r *http.Request) {
	id_str, ok := r.URL.Query()["id"]
	if !ok {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte{})
	}

	id_64, err := strconv.ParseInt(id_str[0], 10, 64)
	if err != nil {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte{})
		return
	}

	id := int(id_64)

	cache_order, isFound := s.cache.Get(id)
	if isFound {
		log.Println("Data from cache")
		tpl_data.Execute(w, cache_order)
		return
	}

	order, err := s.db.GetDataById(id)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte{})
		return
	}

	if len(order.OrderUid) != 0 {
		s.cache.Set(id, order, 5*time.Minute)

		log.Println("Data from db")
		tpl_data.Execute(w, order)
	} else {
		log.Println("Data not found")
		tpl_data_not_found.Execute(w, order)
	}
}

func (s *Service) RestoreCache() error {

	ids, err := s.db.GetIdList()
	if err != nil {
		return err
	}

	for _, id := range ids {
		order, err := s.db.GetDataById(id)
		if err != nil {
			return err
		}
		s.cache.Set(id, order, 5*time.Minute)
	}

	log.Println("cache restored")
	return nil
}

func (s *Service) AddNewOrder(json_bytes []byte) error {
	order := models.Order{}
	if err := json.Unmarshal(json_bytes, &order); err != nil {
		return err
	}
	return s.db.Insert(order)
}
