BEGIN;

DO $$
DECLARE
    v_customer_id BIGINT;
    v_product_id BIGINT;
    v_order_id BIGINT;
BEGIN
    -- Avoid inserting the same demo sale twice.
    IF EXISTS (
        SELECT 1
        FROM commerce.payments
        WHERE payment_reference = 'DEMO-PAY-001'
    ) THEN
        RAISE NOTICE 'Demo sale already exists: skipped.';
        RETURN;
    END IF;

    INSERT INTO commerce.customers (full_name, email, city)
    VALUES ('Client Demo', 'client.demo@example.com', 'Casablanca')
    RETURNING customer_id INTO v_customer_id;

    INSERT INTO commerce.products (sku, product_name, category, price)
    VALUES ('DEMO-KB-001', 'Clavier Demo', 'Informatique', 250.00)
    RETURNING product_id INTO v_product_id;

    INSERT INTO commerce.inventory (product_id, quantity)
    VALUES (v_product_id, 100);

    INSERT INTO commerce.orders (customer_id, status, amount)
    VALUES (v_customer_id, 'CREATED', 500.00)
    RETURNING order_id INTO v_order_id;

    INSERT INTO commerce.order_items (
        order_id, product_id, quantity, unit_price
    )
    VALUES (v_order_id, v_product_id, 2, 250.00);

    INSERT INTO commerce.payments (
        order_id, payment_reference, amount, method, status
    )
    VALUES (v_order_id, 'DEMO-PAY-001', 500.00, 'CARD', 'SUCCESS');

    UPDATE commerce.inventory
    SET quantity = quantity - 2
    WHERE product_id = v_product_id
      AND quantity >= 2;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'Insufficient stock';
    END IF;

    UPDATE commerce.orders
    SET status = 'PAID'
    WHERE order_id = v_order_id;
END;
$$;

COMMIT;
