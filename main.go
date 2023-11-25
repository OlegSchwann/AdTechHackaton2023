package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"

	"golang.org/x/sync/errgroup"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

const (
	// language=PostgreSQL
	initSQL = `
create table if not exists "user"(
	id int primary key not null,
	mail text,
	phone_number text
);

create table if not exists partner(
	id int primary key not null,
	headline text not null,
	description text not null,
	location point not null,
	price_level smallint check ( price_level between 1 and 5)
);

create table if not exists category(
	id int primary key not null,
	parent_id int null references category(id),
	name text not null
);

insert into category(id, parent_id, name) values
    ( 0, null, 'Root'),
	( 1, 0, 'Eating out'),
    ( 2, 0, 'Supermarkets'),
    ( 3, 0, 'Clothes & etc.'),
    ( 4, 0, 'Entertainment'),
    ( 5, 0, 'Transport'),
    ( 6, 0, 'Health & Beauty'),
	( 7, 1, 'Bars'),
	( 8, 1, 'Restaurants'),
	( 9, 1, 'Cafe'),
	(10, 1, 'Burgers'),
	(11, 1, 'Gyros')
ON CONFLICT DO NOTHING;

create table if not exists promotion(
    id int primary key not null,
    partner_id int not null references partner(id),
    category_id int not null references category(id),
    title text not null,
    description text not null
    -- headline_banner
);

create table if not exists action(
	id int primary key not null,
	"type" text not null check (type in ('taken', 'expended')),
	user_id int not null references "user"(id),
	promotion_id int not null references promotion(id)
);

create table if not exists headline_banner(
	url text primary key,
	partner_id int null references partner(id),
	action_id int null references promotion(id),
	category_id int null references category(id),
	image bytea not null
);`

	// language=PostgreSQL
	categoriesSQL = `
select category.id, category.name, headline_banner.url
from category
left join headline_banner on category.id = headline_banner.category_id
where category.parent_id = ?::int;`

	// language=PostgreSQL
	partnersSQL = `
select id, headline, description, location[0] as latitude, location[1] as longitude, price_level
from partner;`

	// language=PostgreSQL
	bannerURLsSQL = `
select url, partner_id 
from headline_banner;`

	// language=PostgreSQL
	bannerImagesSQL = `
select image 
from headline_banner 
where url = $1`
)

type Storage struct {
	db *gorm.DB
}

func NewStorage(db *gorm.DB) (*Storage, error) {
	if err := db.Exec(initSQL).Error; err != nil {
		return nil, fmt.Errorf("NewStorage: %w", err)
	}
	return &Storage{db: db}, nil
}

type Category struct {
	Id   string `json:"id" gorm:"id"`
	Name string `json:"name" gorm:"name"`
	URL  string `json:"url" gorm:"url"`
}

func (s *Storage) GetCategories(parentId int) (*[]Category, error) {
	var categories []Category
	if err := s.db.Raw(categoriesSQL, parentId).Scan(&categories).Error; err != nil {
		return nil, fmt.Errorf("GetCategories: %w", err)
	}
	return &categories, nil
}

type Partner struct {
	Id          int      `json:"id" gorm:"id"`
	Headline    string   `json:"headline" gorm:"headline"`
	Description string   `json:"description" gorm:"description"`
	Latitude    float64  `json:"latitude" gorm:"latitude"`
	Longitude   float64  `json:"longitude" gorm:"longitude"`
	PriceLevel  int8     `json:"price_level" gorm:"price_level"`
	BannerURLs  []string `json:"urls"`
}

func (s *Storage) GetPartners() ([]Partner, error) {
	var partners []Partner
	if err := s.db.Raw(partnersSQL).Scan(&partners).Error; err != nil {
		return nil, fmt.Errorf("GetPartners: %w", err)
	}
	return partners, nil
}

type HeadlineBanner struct {
	URL        string `json:"url" gorm:"url"`
	PartnerID  int    `json:"partner_id" gorm:"partner_id"`
	ActionID   int    `json:"action_id" gorm:"action_id"`
	CategoryID int    `json:"category_id" gorm:"category_id"`
	Image      []byte `json:"image" gorm:"image"`
}

func (s *Storage) GetBannerImageByURL(url string) ([]byte, error) {
	conn, err := s.db.DB()
	if err != nil {
		return nil, err
	}

	var image []byte
	if err = conn.QueryRow(bannerImagesSQL, url).Scan(&image); err != nil {
		return nil, fmt.Errorf("GetBannerURLs: %w", err)
	}

	return image, nil
}

func (s *Storage) GetBannerURLs() (map[int][]string, error) {
	var banners []HeadlineBanner
	if err := s.db.Raw(bannerURLsSQL).Scan(&banners).Error; err != nil {
		return nil, fmt.Errorf("GetBannerURLs: %w", err)
	}

	bannersMap := make(map[int][]string, len(banners))
	for _, b := range banners {
		bannersMap[b.PartnerID] = append(bannersMap[b.PartnerID], b.URL)
	}

	return bannersMap, nil
}

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
	parentId, err := strconv.Atoi(r.URL.Query().Get("parent"))

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
	g, _ := errgroup.WithContext(context.Background())
	var partners []Partner
	g.Go(func() (err error) {
		partners, err = a.storage.GetPartners()
		return err
	})

	var banners map[int][]string
	g.Go(func() (err error) {
		banners, err = a.storage.GetBannerURLs() // TODO: use normal join, not select * from headlie_banners;
		return err
	})

	if err := g.Wait(); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(err.Error()))
		return
	}

	if banners != nil {
		for i := range partners {
			partners[i].BannerURLs = banners[partners[i].Id]
		}
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

func main() {
	// POSTGRESQL="host=localhost port=5432 user=postgres password=first_iteration dbname=default sslmode=disable"
	db, err := gorm.Open(postgres.Open(os.Getenv("POSTGRESQL")), &gorm.Config{})
	if err != nil {
		log.Fatalf("gorm.Open(postgres.Open(%q): %s", os.Getenv("POSTGRESQL"), err.Error())
	}

	storage, err := NewStorage(db)
	if err != nil {
		log.Fatalf("NewStorage: %s", err.Error())
	}

	app := Application{storage: storage}
	http.HandleFunc("/healthcheck", app.HealthCheck)
	http.HandleFunc("/categories", app.GetCategories)
	http.HandleFunc("/partners", app.GetPartners)
	http.HandleFunc("/image", app.GetImage)

	// LISTEN=:8080
	if err := http.ListenAndServe(os.Getenv("LISTEN"), nil); err != nil {
		log.Fatalf("http.ListenAndServe(%q): %s", os.Getenv("LISTEN"), err.Error())
	}
}
