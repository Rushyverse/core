-- Create table to store guilds
CREATE TABLE guild
(
    id         SERIAL                   NOT NULL,
    name       VARCHAR(50)              NOT NULL,
    owner      UUID                     NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    PRIMARY KEY (id)
);

-- Add index on name to be able to search guilds by name
CREATE INDEX idx_guild_name ON guild (name);

-- Create table to store guild members
CREATE TABLE guild_member
(
    guild_id   INTEGER                  NOT NULL,
    member_id  UUID                     NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    PRIMARY KEY (guild_id, member_id),
    FOREIGN KEY (guild_id) REFERENCES guild (id) ON DELETE CASCADE ON UPDATE CASCADE
);

-- Create table to store guild invites
CREATE TABLE guild_invite
(
    guild_id   INTEGER                  NOT NULL,
    member_id  UUID                     NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL,
    expired_at TIMESTAMP WITH TIME ZONE
        constraint ck_guild_invite_expired_at CHECK (expired_at IS NULL OR expired_at > NOW()),
    PRIMARY KEY (guild_id, member_id),
    FOREIGN KEY (guild_id) REFERENCES guild (id) ON DELETE CASCADE ON UPDATE CASCADE
);

-- Add index on expired_at to be able to delete expired invites
CREATE INDEX idx_guild_invite_expired_at ON guild_invite (expired_at);

-- Function to insert guild owner as member
CREATE OR REPLACE FUNCTION insert_owner_as_member() RETURNS TRIGGER AS
$$
BEGIN
    INSERT INTO guild_member(guild_id, member_id, created_at)
    VALUES (NEW.id, NEW.owner, NEW.created_at);
    RETURN NEW;
END
$$ LANGUAGE plpgsql;

-- Trigger to insert guild owner as member on guild creation
CREATE TRIGGER insert_owner_as_member_trigger
    AFTER INSERT
    ON guild
    FOR EACH ROW
EXECUTE PROCEDURE insert_owner_as_member();

-- Function to delete invite if member joins guild
CREATE OR REPLACE FUNCTION delete_invite() RETURNS TRIGGER AS
$$
BEGIN
    DELETE
    FROM guild_invite
    WHERE guild_id = NEW.guild_id
      AND member_id = NEW.member_id;
    RETURN NEW;
END
$$ LANGUAGE plpgsql;

-- Trigger to delete invite if member joins guild
CREATE TRIGGER delete_invite_trigger
    AFTER INSERT
    ON guild_member
    FOR EACH ROW
EXECUTE PROCEDURE delete_invite();

-- Function to check if member is already in guild
-- If member is already in guild, do not insert invite
-- If member is not in guild, insert invite
CREATE OR REPLACE FUNCTION check_existing_member() RETURNS TRIGGER AS
$$
BEGIN
    IF EXISTS(SELECT 1 FROM guild_member WHERE guild_id = NEW.guild_id AND member_id = NEW.member_id) THEN
        RETURN NULL;
    ELSE
        RETURN NEW;
    END IF;
END
$$ LANGUAGE plpgsql;

-- Trigger to check if member is already in guild
CREATE TRIGGER check_existing_member_trigger
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
      AND expired_at < now();
    RETURN NEW;
END
$$ LANGUAGE plpgsql;

-- Trigger to delete expired invites
-- Will be executed after a transaction is committed
CREATE TRIGGER delete_expired_invite_trigger
    AFTER INSERT OR UPDATE
    ON guild_invite
    FOR EACH STATEMENT
EXECUTE PROCEDURE delete_expired_invite();