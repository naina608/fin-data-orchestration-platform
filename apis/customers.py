from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import List, Optional
from core.db import get_snowflake_conn
from datetime import datetime

router = APIRouter()


class Customer(BaseModel):
    customer_id: str
    first_name: str
    last_name: str
    email: str
    created_at: Optional[str] = None
    effective_from: Optional[str] = None
    effective_to: Optional[str] = None
    is_current: Optional[bool] = None

    """
    Pydantic model representing a Customer entity.

    Attributes:
        customer_id (str): Unique identifier for the customer.
        first_name (str): Customer's first name.
        last_name (str): Customer's last name.
        email (str): Customer's email address.
        created_at (Optional[str]): Timestamp when the customer record was created.
        effective_from (Optional[str]): Timestamp when this record became effective.
        effective_to (Optional[str]): Timestamp when this record was superseded or ended.
        is_current (Optional[bool]): Flag indicating if this is the current active record.
    """


@router.get("/customers", response_model=List[Customer])
def get_customers():
    """
    Retrieve all current customers from the database.

    Returns:
        List[Customer]: A list of Customer objects representing all current customers.

    Raises:
        HTTPException: If there is an error fetching customers.
    """
    conn = get_snowflake_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT 
            customer_id, first_name, last_name, email, 
            TO_VARCHAR(created_at), TO_VARCHAR(effective_from), TO_VARCHAR(effective_to), is_current
        FROM dim_customers
        WHERE is_current = TRUE
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()

    return [
        Customer(
            customer_id=r[0],
            first_name=r[1],
            last_name=r[2],
            email=r[3],
            created_at=r[4],
            effective_from=r[5],
            effective_to=r[6],
            is_current=r[7]
        )
        for r in rows
    ]


@router.get("/customers/{customer_id}", response_model=Customer)
def get_customer(customer_id: str):
    """
    Retrieve a single current customer by their customer_id.

    Args:
        customer_id (str): The unique identifier of the customer to retrieve.

    Returns:
        Customer: The Customer object corresponding to the given customer_id.

    Raises:
        HTTPException: 404 if the customer is not found.
    """
    conn = get_snowflake_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT 
            customer_id, first_name, last_name, email, 
            TO_VARCHAR(created_at), TO_VARCHAR(effective_from), TO_VARCHAR(effective_to), is_current
        FROM dim_customers
        WHERE customer_id = %s AND is_current = TRUE
    """, (customer_id,))
    row = cur.fetchone()
    cur.close()
    conn.close()

    if row:
        return Customer(
            customer_id=row[0],
            first_name=row[1],
            last_name=row[2],
            email=row[3],
            created_at=row[4],
            effective_from=row[5],
            effective_to=row[6],
            is_current=row[7]
        )
    else:
        raise HTTPException(status_code=404, detail="Customer not found")


@router.post("/customers", response_model=Customer)
def create_customer(customer: Customer):
    """
    Create a new customer record.

    Args:
        customer (Customer): A Customer object containing details of the new customer.

    Returns:
        Customer: The created Customer object with timestamps set.

    Raises:
        HTTPException: If the creation fails.
    """
    conn = get_snowflake_conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO dim_customers 
        (customer_id, first_name, last_name, email, created_at, effective_from, effective_to, is_current)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        customer.customer_id,
        customer.first_name,
        customer.last_name,
        customer.email,
        customer.created_at or datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
        customer.effective_from or datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
        customer.effective_to or None,
        customer.is_current
    ))
    conn.commit()
    cur.close()
    conn.close()
    return customer


@router.put("/customers/{customer_id}", response_model=Customer)
def update_customer(customer_id: str, customer: Customer):
    """
    Update an existing customer record.

    Args:
        customer_id (str): The unique identifier of the customer to update.
        customer (Customer): A Customer object containing updated customer details.

    Returns:
        Customer: The updated Customer object.

    Raises:
        HTTPException: 404 if the customer to update does not exist.
    """
    conn = get_snowflake_conn()
    cur = conn.cursor()
    cur.execute("""
        UPDATE dim_customers SET 
            first_name=%s,
            last_name=%s,
            email=%s,
            created_at=%s,
            effective_from=%s,
            effective_to=%s,
            is_current=%s
        WHERE customer_id=%s
    """, (
        customer.first_name,
        customer.last_name,
        customer.email,
        customer.created_at or datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
        customer.effective_from or datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
        customer.effective_to,
        customer.is_current,
        customer_id
    ))
    if cur.rowcount == 0:
        cur.close()
        conn.close()
        raise HTTPException(status_code=404, detail="Customer not found")
    conn.commit()
    cur.close()
    conn.close()
    return customer


@router.delete("/customers/{customer_id}")
def delete_customer(customer_id: str):
    """
    Delete a customer record from the database.

    Args:
        customer_id (str): The unique identifier of the customer to delete.

    Returns:
        dict: A message confirming successful deletion.

    Raises:
        HTTPException: 404 if the customer to delete does not exist.
    """
    conn = get_snowflake_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM dim_customers WHERE customer_id = %s", (customer_id,))
    if cur.rowcount == 0:
        cur.close()
        conn.close()
        raise HTTPException(status_code=404, detail="Customer not found")
    conn.commit()
    cur.close()
    conn.close()
    return {"detail": "Customer deleted successfully"}
