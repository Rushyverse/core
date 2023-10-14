CREATE TYPE rank AS ENUM ('PLAYER', 'ADMIN');

-- Create table to store players
CREATE TABLE player
(
    uuid     uuid PRIMARY KEY,
    rank     rank NOT NULL,
    language varchar(50) NOT NULL
);
