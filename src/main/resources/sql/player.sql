
-- CREATE TYPE rank AS ENUM ('PLAYER', 'ADMIN');

-- Create table to store players
CREATE TABLE player
(
    uuid uuid PRIMARY KEY,
    rank varchar(50) NOT NULL -- TODO: Change to ENUM when Komapper supports it
--     rank rank NOT NULL
);
