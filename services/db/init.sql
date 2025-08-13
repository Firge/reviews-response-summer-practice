CREATE TABLE IF NOT EXISTS reviews (
    id SERIAL PRIMARY KEY,
    content TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS responses (
    id SERIAL PRIMARY KEY,
    review_id INTEGER REFERENCES reviews(id),
    content TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Тестовые данные
INSERT INTO reviews (content) VALUES
('Отличный товар, всё понравилось!'),
('Не работает, верните деньги.'),
('Средний продукт, ничего особенного.'),
('Быстрая доставка, спасибо!'),
('Качество не соответствует описанию.');