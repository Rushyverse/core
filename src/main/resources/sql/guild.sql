-- Create table to store guilds
CREATE TABLE guild
(
    id         SERIAL                   NOT NULL,
    name       VARCHAR(50)              NOT NULL
        CONSTRAINT ck_guild_name_not_empty CHECK (name <> ''),
    owner_id   VARCHAR(50)              NOT NULL
        CONSTRAINT ck_guild_owner_id_not_empty CHECK (owner_id <> ''), -- TODO Replace by foreign key to entity table
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    PRIMARY KEY (id)
);

-- Add index on name to be able to search guilds by name
CREATE INDEX idx_guild_name ON guild (name);

-- Create table to store guild members
CREATE TABLE guild_member
(
    guild_id   INTEGER                  NOT NULL,
    entity_id  VARCHAR(50)              NOT NULL
        CONSTRAINT ck_guild_member_entity_id_not_empty CHECK (entity_id <> ''), -- TODO Replace by foreign key to entity table
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    PRIMARY KEY (guild_id, entity_id),
    FOREIGN KEY (guild_id) REFERENCES guild (id) ON DELETE CASCADE ON UPDATE CASCADE
);

-- Create table to store guild invites
CREATE TABLE guild_invite
(
    guild_id   INTEGER                  NOT NULL,
    entity_id  VARCHAR(50)              NOT NULL
        CONSTRAINT ck_guild_invite_entity_id_not_empty CHECK (entity_id <> ''), -- TODO Replace by foreign key to entity table
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    expired_at TIMESTAMP WITH TIME ZONE
        constraint ck_guild_invite_expired_at CHECK (expired_at IS NULL OR expired_at > NOW()),
    PRIMARY KEY (guild_id, entity_id),
    FOREIGN KEY (guild_id) REFERENCES guild (id) ON DELETE CASCADE ON UPDATE CASCADE
);

-- Add index on expired_at to be able to delete expired invites
CREATE INDEX idx_guild_invite_expired_at ON guild_invite (expired_at);

-- Create view to get guilds with members with owner
CREATE OR REPLACE VIEW guild_members_with_owner AS
SELECT g.id as guild_id, g.owner_id as member_id, g.created_at as created_at
FROM guild g
UNION ALL
SELECT gm.guild_id as guild_id, gm.entity_id as member_id, gm.created_at as created_at
FROM guild_member gm;

-- Function to check if member is the owner of the guild
CREATE OR REPLACE FUNCTION check_member_is_owner() RETURNS TRIGGER AS
$$
BEGIN
    IF EXISTS(SELECT 1
              FROM guild g
              WHERE g.id = NEW.guild_id
                AND g.owner_id = NEW.entity_id) THEN
        RAISE EXCEPTION USING
            ERRCODE = 'P1001',
            MESSAGE = 'The entity cannot be set as a member of the guild because he is the owner';
    ELSE
        RETURN NEW;
    END IF;
END
$$ LANGUAGE plpgsql;

-- Trigger to check if member is is the owner of the guild before adding a member to a guild
CREATE OR REPLACE TRIGGER check_member_is_owner_trigger
    BEFORE INSERT
    ON guild_member
    FOR EACH ROW
EXECUTE FUNCTION check_member_is_owner();

-- Function to delete invite if member joins guild
CREATE OR REPLACE FUNCTION delete_invite() RETURNS TRIGGER AS
$$
BEGIN
    DELETE
    FROM guild_invite
    WHERE guild_id = NEW.guild_id
      AND entity_id = NEW.entity_id;
    RETURN NEW;
END
$$ LANGUAGE plpgsql;

-- Trigger to delete invite if member joins guild
CREATE OR REPLACE TRIGGER delete_invite_trigger
    AFTER INSERT
    ON guild_member
    FOR EACH ROW
EXECUTE PROCEDURE delete_invite();

-- Create a function to check if the entity is a member of the guild
-- First parameter is the guild id
-- Second parameter is the entity id
-- Returns a table with 1 if the entity is a member of the guild, an empty table otherwise
CREATE OR REPLACE FUNCTION is_member(guild INTEGER, entity VARCHAR(50))
    RETURNS TABLE
            (
                is_member INTEGER
            )
AS
$$
BEGIN
    RETURN QUERY (SELECT 1
                  FROM guild_members_with_owner g
                  WHERE g.guild_id = guild
                    AND g.member_id = entity);
END
$$ LANGUAGE plpgsql;

-- Function to check if member is already in guild
-- If member is already in guild, do not insert invite
-- If member is not in guild, insert invite
CREATE OR REPLACE FUNCTION check_existing_member() RETURNS TRIGGER AS
$$
BEGIN
    IF EXISTS(SELECT 1 FROM is_member(NEW.guild_id, NEW.entity_id)) THEN
        RAISE EXCEPTION USING
            ERRCODE = 'P1000',
            MESSAGE = 'The entity cannot be invited to the guild because he is already a member of it';
    ELSE
        RETURN NEW;
    END IF;
END
$$ LANGUAGE plpgsql;

-- Trigger to check if member is already in guild
CREATE OR REPLACE TRIGGER check_existing_member_trigger
    BEFORE INSERT
    ON guild_invite
    FOR EACH ROW
EXECUTE FUNCTION check_existing_member();

-- Function to delete expired invites
-- Will delete rows where expired_at is not null and is in the past
CREATE OR REPLACE FUNCTION delete_expired_invite() RETURNS TRIGGER AS
$$
BEGIN
    DELETE
    FROM guild_invite
    WHERE expired_at IS NOT NULL
      AND expired_at < NOW();
    RETURN NEW;
END
$$ LANGUAGE plpgsql;

-- Trigger to delete expired invites
-- Will be executed after a transaction is committed
CREATE OR REPLACE TRIGGER delete_expired_invite_trigger
    AFTER INSERT OR UPDATE
    ON guild_invite
    FOR EACH STATEMENT
EXECUTE PROCEDURE delete_expired_invite();