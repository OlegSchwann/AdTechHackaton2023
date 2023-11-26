package main

import (
	"fmt"
	"gorm.io/gorm"
)

const (
	// language=PostgreSQL
	initSQL = `
-- create extension if not exists postgis; ?

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
	-- + headline_banner
);

create table if not exists category(
	id int primary key not null,
	parent_id int null references category(id),
	name text not null
	-- + headline_banner
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
	-- + headline_banner
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
	promotion_id int null references promotion(id),
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
select
    partner.id, partner.headline, partner.description,
    partner.location[0] as latitude, partner.location[1] as longitude,
    partner.price_level, headline_banner.url
from partner
left join public.headline_banner on partner.id = headline_banner.partner_id;`

	// language=PostgreSQL
	bannerImagesSQL = `
select image 
from headline_banner 
where url = $1;`

	// language=PostgreSQL
	promotionByPartnerSQL = `
select
    promotion.id, 
	promotion.title,
	promotion.description,
	headline_banner.url
from promotion
left join headline_banner on promotion.id = headline_banner.promotion_id
where
    ?::int = 0 OR
    promotion.partner_id = ?::int;`

	// language=PostgreSQL
	promotionByGeoSQL = `
select
    promotion.id, 
	promotion.title,
	promotion.description,
	headline_banner.url
from promotion
left join partner on promotion.partner_id = partner.id
left join headline_banner on promotion.id = headline_banner.promotion_id
order by partner.location <-> ST_SetSRID(ST_MakePoint(?::float, ?::float),4326) desc -- TODO: Test it
;`
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
	Id   int    `json:"id" gorm:"id"`
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
	Id          int     `json:"id" gorm:"id"`
	Headline    string  `json:"headline" gorm:"headline"`
	Description string  `json:"description" gorm:"description"`
	Latitude    float64 `json:"latitude" gorm:"latitude"`
	Longitude   float64 `json:"longitude" gorm:"longitude"`
	PriceLevel  int8    `json:"price_level" gorm:"price_level"`
	BannerURL   string  `json:"headline_banner_url" gorm:"url"`
}

func (s *Storage) GetPartners() ([]Partner, error) {
	var partners []Partner
	if err := s.db.Raw(partnersSQL).Scan(&partners).Error; err != nil {
		return nil, fmt.Errorf("GetPartners: %w", err)
	}
	return partners, nil
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

type Promotion struct {
	Id          int    `json:"id" gorm:"id"`
	Title       string `json:"title" gorm:"title"`
	Description string `json:"description" gorm:"description"`
	Url         string `json:"headline_banner_url" gorm:"url"`
}

func (s *Storage) GetPromotionsByPartner(partner int) ([]Promotion, error) {
	var promotions []Promotion
	if err := s.db.Raw(promotionByPartnerSQL, partner, partner).Scan(&promotions); err != nil {
		return nil, fmt.Errorf("GetPromotionsByPartner: %w", err)
	}
	return promotions, nil
}

func (s *Storage) GetPromotionsByGeo(long, lat float64) ([]Promotion, error) {
	var promotions []Promotion
	if err := s.db.Raw(promotionByGeoSQL, long, lat).Scan(&promotions); err != nil {
		return nil, fmt.Errorf("GetPromotionsByGeo: %w", err)
	}
	return promotions, nil
}
