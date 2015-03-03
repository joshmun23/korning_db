-- DEFINE YOUR DATABASE SCHEMA HERE
create table employees (
	id SERIAL primary key,
	name varchar(250),
	email varchar(250)
);

create table customers (
	id SERIAL primary key,
	name varchar(250),
	account_no int
);

create table products (
	id SERIAL primary key,
	product varchar(250)
);

create table invoices (
	id SERIAL primary key,
	invoice_frequency varchar(250)
);

create table sales (
	id SERIAL primary key,
	sale_date date,
	sale_amount real,
	units_sold int,
	invoice_no int,
	product_id int REFERENCES products (id),
	invoice_id int REFERENCES invoices (id),
	customer_id int REFERENCES customers (id),
	employee_id int REFERENCES employees (id)
);