package main

import (
	"encoding/json"
	"net/http"
	"strconv"
)

type Application struct {
	storage *Storage
}

func (a *Application) HealthCheck(w http.ResponseWriter, r *http.Request) {
	db, _ := a.storage.db.DB()
	err := db.Ping()
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}
	w.Write([]byte("ok"))
}

func (a *Application) GetCategories(w http.ResponseWriter, r *http.Request) {
	parentId, _ := strconv.Atoi(r.URL.Query().Get("parent"))

	categories, err := a.storage.GetCategories(parentId)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}

	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	enc.Encode(categories)
}

func (a *Application) GetPartners(w http.ResponseWriter, _ *http.Request) {
	partners, err := a.storage.GetPartners()
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}

	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	enc.Encode(partners)
}

func (a *Application) GetImage(w http.ResponseWriter, r *http.Request) {
	url := r.URL.Query().Get("url")

	image, err := a.storage.GetBannerImageByURL(url)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}

	w.Header().Set("Content-Type", "image/jpeg")
	w.Write(image)
}

func (a *Application) GetPromotions(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	partner, _ := strconv.Atoi(query.Get("partner"))
	latitude, _ := strconv.ParseFloat(query.Get("lat"), 64)
	longitude, _ := strconv.ParseFloat(query.Get("long"), 64)

	var promotions []Promotion
	var err error
	if latitude != 0 && longitude != 0 {
		promotions, err = a.storage.GetPromotionsByGeo(latitude, longitude)
	} else {
		promotions, err = a.storage.GetPromotionsByPartner(partner)
	}
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}

	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	enc.Encode(promotions)
}
