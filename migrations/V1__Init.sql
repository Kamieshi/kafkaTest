CREATE TABLE messages
(
    id       uuid unique not null primary key,
    pay_load varchar(500)
);
