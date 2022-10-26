-- simulated_orders

drop procedure if exists simulate_orders;

delimeter //
create procedure simulate_orders()
begin
  declare counter integer;
    set counter = 0;
    loop_label: loop
      -- start new order
      start transaction
			
				-- set random values
        set @next_order_id = (select max(orderNumber) + 1 from orders);
        set @random_customer_id = (select customerNumber from customers order by Rand() limit 1);
				set @random_order_status = (select elt(0.5 + rand() * 6, 'On Hold', 'Shipped', 'Resolved', 'In Process', 'Disputed', 'Cancelled'));
				set @random_customer_comment = (select elt(0.5 + rand() * 6, 'Customer requested to be carful', 'No Comments', '', 'Dog in the front yard', 'Leave product by the door', 'Sensitive Product'));
				set @random_product_id = (select productCode from products order by Rand() limit 1);
				set @random_quantity = (select floor(rand() * (100 - 1) + 1));
				set @random_orderline = (select floor(rand() * (18 - 1) + 1));

				-- intert new order into orders table
				insert into orders(orderNumber, orderDate, requireDate, shippedDate, status, comments, customerNumber) 
				values 
					((select @next_order_id), 
					(select date_add(current_date(), interval 7 day)), 
					(select date_add(current_date(), interval 3 day)),
					(select @random_order_status),
					(select @random_customer_comment),
					(select @random_customer_id));

				-- insert new order into ordersdetails table
				insert into orderdetails 
				(orderNumber, productCode, quantityOrdered, priceEach, orderLineNumber)
				values
					((select @next_order_id), 
					(select @random_product_id), 
					(select @random_quantity),
					(select buyPrice from products where productcode = @random_product_id),
					(select @random_orderline));
			commit;
			set counter = counter + 1;
			if mod(counter, 2) = 0 then
				set @random_order_status = (select elt(0.5 + rand() * 6, 'On Hold', 'Shipped', 'Resolved', 'In Process', 'Disputed', 'Cancelled'));
				update orders
				set status = @random_order_status
					where orderNumber = @next_order_id
			end if;

			if mod(counter, 5) = 0 then
				set @random_order_status = (SELECT ELT(0.5 + RAND() * 6, 'On Hold', 'Shipped', 'Resolved', 'In Process', 'Disputed', 'Cancelled'));
                
				delete from orderdetails
        	where orderNumber = @next_order_id;
                
				delete from orders
        	where orderNumber = @next_order_id;
                
			end if;
                  
			# Stop inserting after 1000 orders
			if counter = 1000 then
				leave loop_label;
			end if;

	  end loop;
end //
DELIMITER //
