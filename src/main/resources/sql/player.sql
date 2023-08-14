CREATE TYPE rank AS ENUM ('PLAYER', 'ADMIN');

CREATE TYPE language AS ENUM (
    'ENGLISH',
    'FRENCH',
    'SPANISH',
    'GERMAN',
    'CHINESE'
);

-- Create table to store players
CREATE TABLE player
(
    uuid     uuid PRIMARY KEY,
    rank     varchar(50) NOT NULL, -- TODO: Change to ENUM when Komapper supports it
--     rank rank NOT NULL
    language varchar(50) NOT NULL
--     language language NOT NULL
);
