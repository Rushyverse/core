
CREATE TYPE rank AS ENUM ('PLAYER', 'ADMIN');

-- Create table to store players
CREATE TABLE player
(
    uuid uuid PRIMARY KEY,
    rank rank NOT NULL
);
