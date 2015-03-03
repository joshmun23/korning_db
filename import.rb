# Use this file to import the sales information into the
# the database.
require 'pry'
require "pg"
require 'csv'

def csv_to_db(arr)
  arr.each do |info|
    db_connection do |conn|
      conn.exec("INSERT INTO table_name (name) VALUES ($1)", [info])
    end
  end
end

def db_connection
  begin
    connection = PG.connect(dbname: "korning")
    yield(connection)
  ensure
    connection.close
  end
end

def parse_employee(row)
	employee = {}

  employee[:name] = row["employee"].split(' ')[0..1].join(' ') 
	employee[:email] = row["employee"].split(' ')[2].split('')[1..-2].join('')
	
	employee
end

def parse_customer(row)
	customer = {}

	customer[:name] = row["customer_and_account_no"].split[0]
	customer[:account_no] = row["customer_and_account_no"].split[1][1..-2]
	
	customer
end

def parse_sale(row)
	sale = {}

	sale[:sale_date] = row["sale_date"]
	sale[:sale_amount] = row["sale_amount"].split('')[1..-1].join('').to_f
	sale[:units_sold] = row["units_sold"].to_i
	sale[:invoice_no] = row["invoice_no"].to_i

	sale
end

def parse_invoice_frequency(row)
	invoice_frequency = {}

	invoice_frequency[:invoice_frequency] = row["invoice_frequency"]
end

def parse_product(row)
	product = {}

	product[:product_name] = row["product_name"]
end

def insert_employee(employee_info)
	db_connection do |conn|
		#query database to see if this employee already exists
		result = conn.exec_params("SELECT id FROM employees WHERE email = $1", [employee_info[:email]]).first
		if result.nil?
			#no rows with that email exist, insert the employee and return the id
			conn.exec_params("INSERT INTO employees (name, email) VALUES ($1, $2)", [employee_info[:name], employee_info[:email]]).first
			result = conn.exec_params("SELECT id FROM employees WHERE email = $1", [employee_info[:email]]).first
		end

		# result = { result["name"] => result["id"] }
		result
	end
end


def insert_customer(customer_info)
	db_connection do |conn|
		result = conn.exec_params("SELECT id FROM customers WHERE name = $1", [customer_info[:name]]).first

		if result.nil?
			#no rows with that email exist, insert the employee and return the id
			conn.exec_params("INSERT INTO customers (name, account_no) VALUES ($1, $2)", [customer_info[:name], customer_info[:account_no].to_i]).first
			result = conn.exec_params("SELECT id FROM customers WHERE name = $1", [customer_info[:name]]).first
		end

		
		# result = { result["name"] => result["id"] }
		result
	end
end

def insert_product(product_info)
	db_connection do |conn|
		result = conn.exec_params("SELECT id FROM products WHERE product = $1", [product_info]).first

		if result.nil?
			#no rows with that email exist, insert the employee and return the id
			conn.exec_params("INSERT INTO products (product) VALUES ($1)", [product_info]).first
			result = conn.exec_params("SELECT id FROM products WHERE product = $1", [product_info]).first
		end

		
		# result = { result["product"] => result["id"] }
		result
	end
end

def insert_invoice_frequency(invoice_frequency_info)
	db_connection do |conn|
		result = conn.exec_params("SELECT id FROM invoices WHERE invoice_frequency = $1", [invoice_frequency_info]).first

		if result.nil?
			#no rows with that email exist, insert the employee and return the id
			conn.exec_params("INSERT INTO invoices (invoice_frequency) VALUES ($1)", [invoice_frequency_info]).first
			result = conn.exec_params("SELECT id FROM invoices WHERE invoice_frequency = $1", [invoice_frequency_info]).first
		end
		
		# result = { result["invoice_frequency"] => result["id"] }
		result
	end	
end


def insert_sale(sale, employee_id, customer_id, product_id, invoice_frequency_id)
	db_connection do |conn|
		
		conn.exec_params("INSERT INTO sales (sale_date, sale_amount, units_sold, invoice_no, product_id, invoice_id, customer_id,
		 employee_id) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)", [sale[:sale_date], sale[:sale_amount], sale[:units_sold], sale[:invoice_no],
		 product_id["id"], invoice_frequency_id["id"], customer_id["id"], employee_id["id"]]).first
	end
end

def csv_to_db
	CSV.foreach("sales.csv", headers: true) do |row|
		employee = parse_employee(row) #return a hash
		customer = parse_customer(row) #return a hash
		sale = parse_sale(row)	
		invoice_frequency = parse_invoice_frequency(row)
		product = parse_product(row)

		#run hash from above through insert employee method to return hash where name is associated with id
		employee_id = insert_employee(employee)
		customer_id = insert_customer(customer)
		product_id = insert_product(product)
		invoice_frequency_id = insert_invoice_frequency(invoice_frequency)


		insert_sale(sale, employee_id, customer_id, product_id, invoice_frequency_id)
	end
end

csv_to_db