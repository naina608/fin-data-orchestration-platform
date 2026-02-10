from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import List, Optional
from core.db import get_snowflake_conn
from datetime import datetime

router = APIRouter()


class Transaction(BaseModel):
    transaction_id: str
    account_id: str
    customer_id: str
    amount: float

    related_account_id: Optional[str] = None
    status: Optional[str] = None
    transaction_type: Optional[str] = None
    transaction_time: Optional[str] = None
    load_timestamp: Optional[str] = None

    """
    Pydantic model representing a Transaction entity.

    Attributes:
        transaction_id (str): Unique identifier for the transaction.
        account_id (str): Identifier for the associated account.
        customer_id (str): Identifier for the customer involved in the transaction.
        amount (float): Transaction amount.
        related_account_id (Optional[str]): Identifier for a related account, if any.
        status (Optional[str]): Status of the transaction (e.g., pending, completed).
        transaction_type (Optional[str]): Type of the transaction (e.g., debit, credit).
        transaction_time (Optional[str]): Timestamp when the transaction occurred.
        load_timestamp (Optional[str]): Timestamp when the transaction record was loaded.
    """


@router.get("/transactions", response_model=List[Transaction])
def get_transactions():
    """
    Retrieve all transactions from the database.

    Returns:
        List[Transaction]: A list of Transaction objects representing all transactions.

    Raises:
        HTTPException: If there is an error fetching transactions.
    """
    conn = get_snowflake_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT 
            transaction_id, account_id, customer_id, amount, 
            related_account_id, status, transaction_type, 
            TO_VARCHAR(transaction_time), TO_VARCHAR(load_timestamp)
        FROM fact_transactions
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()

    return [
        Transaction(
            transaction_id=r[0],
            account_id=r[1],
            customer_id=r[2],
            amount=r[3],
            related_account_id=r[4],
            status=r[5],
            transaction_type=r[6],
            transaction_time=r[7],
            load_timestamp=r[8]
        )
        for r in rows
    ]


@router.get("/transactions/{transaction_id}", response_model=Transaction)
def get_transaction(transaction_id: str):
    """
    Retrieve a single transaction by its transaction_id.

    Args:
        transaction_id (str): The unique identifier of the transaction to retrieve.

    Returns:
        Transaction: The Transaction object corresponding to the given transaction_id.

    Raises:
        HTTPException: 404 if the transaction is not found.
    """
    conn = get_snowflake_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT 
            transaction_id, account_id, customer_id, amount, 
            related_account_id, status, transaction_type, 
            TO_VARCHAR(transaction_time), TO_VARCHAR(load_timestamp)
        FROM fact_transactions
        WHERE transaction_id = %s
    """, (transaction_id,))
    row = cur.fetchone()
    cur.close()
    conn.close()

    if row:
        return Transaction(
            transaction_id=row[0],
            account_id=row[1],
            customer_id=row[2],
            amount=row[3],
            related_account_id=row[4],
            status=row[5],
            transaction_type=row[6],
            transaction_time=row[7],
            load_timestamp=row[8]
        )
    else:
        raise HTTPException(status_code=404, detail="Transaction not found")


@router.post("/transactions", response_model=Transaction)
def create_transaction(transaction: Transaction):
    """
    Create a new transaction record.

    Args:
        transaction (Transaction): A Transaction object containing details of the new transaction.

    Returns:
        Transaction: The created Transaction object with timestamps set.

    Raises:
        HTTPException: If the creation fails.
    """
    conn = get_snowflake_conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO fact_transactions 
        (transaction_id, account_id, customer_id, amount, related_account_id, status, transaction_type, transaction_time, load_timestamp)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        transaction.transaction_id,
        transaction.account_id,
        transaction.customer_id,
        transaction.amount,
        transaction.related_account_id,
        transaction.status,
        transaction.transaction_type,
        transaction.transaction_time or datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
        transaction.load_timestamp or datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
    ))
    conn.commit()
    cur.close()
    conn.close()
    return transaction


@router.put("/transactions/{transaction_id}", response_model=Transaction)
def update_transaction(transaction_id: str, transaction: Transaction):
    """
    Update an existing transaction record.

    Args:
        transaction_id (str): The unique identifier of the transaction to update.
        transaction (Transaction): A Transaction object containing updated transaction details.

    Returns:
        Transaction: The updated Transaction object.

    Raises:
        HTTPException: 404 if the transaction to update does not exist.
    """
    conn = get_snowflake_conn()
    cur = conn.cursor()
    cur.execute("""
        UPDATE fact_transactions SET 
            account_id=%s,
            customer_id=%s,
            amount=%s,
            related_account_id=%s,
            status=%s,
            transaction_type=%s,
            transaction_time=%s,
            load_timestamp=%s
        WHERE transaction_id=%s
    """, (
        transaction.account_id,
        transaction.customer_id,
        transaction.amount,
        transaction.related_account_id,
        transaction.status,
        transaction.transaction_type,
        transaction.transaction_time or datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
        transaction.load_timestamp or datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
        transaction_id
    ))
    if cur.rowcount == 0:
        cur.close()
        conn.close()
        raise HTTPException(status_code=404, detail="Transaction not found")
    conn.commit()
    cur.close()
    conn.close()
    return transaction


@router.delete("/transactions/{transaction_id}")
def delete_transaction(transaction_id: str):
    """
    Delete a transaction record from the database.

    Args:
        transaction_id (str): The unique identifier of the transaction to delete.

    Returns:
        dict: A message confirming successful deletion.

    Raises:
        HTTPException: 404 if the transaction to delete does not exist.
    """
    conn = get_snowflake_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM fact_transactions WHERE transaction_id = %s", (transaction_id,))
    if cur.rowcount == 0:
        cur.close()
        conn.close()
        raise HTTPException(status_code=404, detail="Transaction not found")
    conn.commit()
    cur.close()
    conn.close()
    return {"detail": "Transaction deleted successfully"}
