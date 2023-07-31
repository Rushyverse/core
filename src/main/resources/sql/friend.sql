-- Create table to store friends
CREATE TABLE friend
(
    uuid1 uuid NOT NULL,
    uuid2 uuid NOT NULL,
    pending BOOLEAN NOT NULL DEFAULT TRUE,
    PRIMARY KEY (uuid1, uuid2)
);

-- Add index on uuid1 and uuid2 to be able to search friends by uuid
-- and avoid duplicate entries (uuid1, uuid2) and (uuid2, uuid1)
CREATE UNIQUE INDEX friend_unique_idx ON friend (GREATEST(uuid1, uuid2), LEAST(uuid1, uuid2));
