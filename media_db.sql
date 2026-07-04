-- ================================================
-- DATABASE: media_db
-- SCHEMA: 2 tables with PK and FK relationship
-- ================================================

CREATE DATABASE IF NOT EXISTS media_db;
USE media_db;

-- ------------------------------------------------
-- TABLE 1: media_content (PARENT TABLE)
-- Stores movies, series, documentaries
-- ------------------------------------------------
CREATE TABLE IF NOT EXISTS media_content (
    id           INT AUTO_INCREMENT PRIMARY KEY,  -- Primary Key
    title        VARCHAR(200)  NOT NULL,
    description  TEXT,
    type         ENUM('movie', 'series', 'documentary', 'short') NOT NULL,
    genre        VARCHAR(50),
    release_year YEAR,
    duration_min INT,
    rating       DECIMAL(3,1),
    created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ------------------------------------------------
-- TABLE 2: media_reviews (CHILD TABLE)
-- Stores reviews linked to media_content
-- ------------------------------------------------
CREATE TABLE IF NOT EXISTS media_reviews (
    id          INT AUTO_INCREMENT PRIMARY KEY,       -- Primary Key
    content_id  INT          NOT NULL,                -- Foreign Key
    reviewer    VARCHAR(100) NOT NULL,
    score       TINYINT      NOT NULL CHECK (score BETWEEN 1 AND 10),
    review_text TEXT,
    reviewed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_content FOREIGN KEY (content_id)   -- FK links to media_content.id
        REFERENCES media_content(id)
        ON DELETE CASCADE                             -- delete reviews if media deleted
        ON UPDATE CASCADE                             -- update if media id changes
);

-- ------------------------------------------------
-- INSERTS: media_content (parent first)
-- ------------------------------------------------
INSERT INTO media_content (title, description, type, genre, release_year, duration_min, rating) VALUES
('The Lion King',  'A young lion prince grows up to reclaim his kingdom', 'movie',       'Animation', 1994, 88,  8.5),
('Breaking Bad',   'A chemistry teacher turns into a drug lord',           'series',      'Crime',     2008, 47,  9.5),
('Planet Earth',   'A stunning look at life on our planet',                'documentary', 'Nature',    2006, 50,  9.4),
('Inception',      'A thief who enters dreams to steal secrets',           'movie',       'Sci-Fi',    2010, 148, 8.8),
('The Office',     'Everyday life of office employees, full of humor',     'series',      'Comedy',    2005, 22,  9.0);

-- ------------------------------------------------
-- INSERTS: media_reviews (child after parent)
-- content_id MUST match an existing id in media_content
-- ------------------------------------------------
INSERT INTO media_reviews (content_id, reviewer, score, review_text) VALUES
(1, 'John',    9,  'A classic! My kids watch it every week.'),
(1, 'Sarah',   8,  'Beautiful animation and great music.'),
(2, 'Mike',    10, 'Best TV show ever made. Period.'),
(2, 'Emily',   9,  'Intense and gripping from start to finish.'),
(3, 'David',   10, 'Absolutely breathtaking visuals of nature.'),
(4, 'Anna',    8,  'Mind-bending story, watch it twice!'),
(4, 'Chris',   9,  'Christopher Nolan at his best.'),
(5, 'Jessica', 10, 'So funny, I laughed every single episode.'),
(5, 'Tom',     8,  'Great show to relax and unwind.');
