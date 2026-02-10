from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import List, Optional
from core.db import get_snowflake_conn
from datetime import datetime

router = APIRouter()


class Account(BaseModel):
    account_id: str
    customer_id: str
    account_type: str
    balance: float
    currency: str

    created_at: Optional[str] = None
    effective_from: Optional[str] = None
    effective_to: Optional[str] = None
    is_current: Optional[bool] = None
    """
    Pydantic model representing an Account entity.

    Attributes:
        account_id (str): Unique identifier for the account.
        customer_id (str): Identifier for the customer owning the account.
        account_type (str): Type of the account (e.g., savings, checking).
        balance (float): Current balance of the account.
        currency (str): Currency code for the balance (e.g., USD).
        created_at (Optional[str]): Timestamp when the account was created.
        effective_from (Optional[str]): Timestamp when this record became effective.
        effective_to (Optional[str]): Timestamp when this record was superseded.
        is_current (Optional[bool]): Flag indicating if this is the current record.
    """


@router.get("/accounts", response_model=List[Account])
def get_accounts():
    """
    Retrieve all current accounts from the database.

    Returns:
        List[Account]: A list of Account objects representing all current accounts.

    Raises:
        HTTPException: If there is an error fetching accounts.
    """
    conn = get_snowflake_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT account_id, customer_id, account_type, balance, currency, 
               TO_VARCHAR(created_at), TO_VARCHAR(effective_from), TO_VARCHAR(effective_to), is_current 
        FROM dim_accounts 
        WHERE is_current = TRUE
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return [
        Account(
            account_id=r[0],
            customer_id=r[1],
            account_type=r[2],
            balance=r[3],
            currency=r[4],
            created_at=r[5],
            effective_from=r[6],
            effective_to=r[7],
            is_current=r[8]
        )
        for r in rows
    ]


@router.get("/accounts/{account_id}", response_model=Account)
def get_account(account_id: str):
    """
    Retrieve a single current account by its account_id.

    Args:
        account_id (str): The unique identifier of the account to retrieve.

    Returns:
        Account: The Account object corresponding to the given account_id.

    Raises:
        HTTPException: 404 if the account is not found.
    """
    conn = get_snowflake_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT account_id, customer_id, account_type, balance, currency, 
               TO_VARCHAR(created_at), TO_VARCHAR(effective_from), TO_VARCHAR(effective_to), is_current 
        FROM dim_accounts 
        WHERE account_id = %s AND is_current = TRUE
    """, (account_id,))
    row = cur.fetchone()
    cur.close()
    conn.close()
    if row:
        return Account(
            account_id=row[0],
            customer_id=row[1],
            account_type=row[2],
            balance=row[3],
            currency=row[4],
            created_at=row[5],
            effective_from=row[6],
            effective_to=row[7],
            is_current=row[8]
        )
    else:
        raise HTTPException(status_code=404, detail="Account not found")


@router.post("/accounts", response_model=Account)
def create_account(account: Account):
    """
    Create a new account record.

    Args:
        account (Account): An Account object containing the details of the new account.

    Returns:
        Account: The created Account object with timestamps set.

    Raises:
        HTTPException: If the creation fails.
    """
    conn = get_snowflake_conn()
    cur = conn.cursor()
    created_at = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    cur.execute("""
        INSERT INTO dim_accounts (account_id, customer_id, account_type, balance, currency, created_at, effective_from, is_current)
        VALUES (%s, %s, %s, %s, %s, %s, %s, TRUE)
    """, (
        account.account_id,
        account.customer_id,
        account.account_type,
        account.balance,
        account.currency,
        created_at,
        created_at
    ))
    conn.commit()
    cur.close()
    conn.close()
    account.created_at = created_at
    account.effective_from = created_at
    return account


@router.put("/accounts/{account_id}", response_model=Account)
def update_account(account_id: str, account: Account):
    """
    Update an existing account by marking the old record as not current and inserting a new current record.

    Args:
        account_id (str): The unique identifier of the account to update.
        account (Account): An Account object containing updated account details.

    Returns:
        Account: The updated Account object with refreshed effective dates.

    Raises:
        HTTPException: 404 if the account to update does not exist.
    """
    conn = get_snowflake_conn()
    cur = conn.cursor()
    # Check if account exists
    cur.execute("SELECT account_id FROM dim_accounts WHERE account_id = %s AND is_current = TRUE", (account_id,))
    if not cur.fetchone():
        cur.close()
        conn.close()
        raise HTTPException(status_code=404, detail="Account not found")

    # Mark old record as not current
    cur.execute(
        "UPDATE dim_accounts SET is_current = FALSE, effective_to = %s WHERE account_id = %s AND is_current = TRUE",
        (datetime.utcnow(), account_id))

    # Insert new record as current
    cur.execute("""
        INSERT INTO dim_accounts (account_id, customer_id, account_type, balance, currency, created_at, effective_from, is_current)
        VALUES (%s, %s, %s, %s, %s, %s, %s, TRUE)
    """, (
        account.account_id,
        account.customer_id,
        account.account_type,
        account.balance,
        account.currency,
        account.created_at or datetime.utcnow(),
        datetime.utcnow()
    ))
    conn.commit()
    cur.close()
    conn.close()
    account.effective_from = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    account.is_current = True
    return account


@router.delete("/accounts/{account_id}")
def delete_account(account_id: str):
    """
    Soft delete an account by marking it as not current.

    Args:
        account_id (str): The unique identifier of the account to delete.

    Returns:
        dict: A message confirming successful deletion.

    Raises:
        HTTPException: 404 if the account to delete does not exist.
    """
    conn = get_snowflake_conn()
    cur = conn.cursor()
    # Soft delete: mark as not current
    cur.execute(
        "UPDATE dim_accounts SET is_current = FALSE, effective_to = %s WHERE account_id = %s AND is_current = TRUE",
        (datetime.utcnow(), account_id))
    if cur.rowcount == 0:
        cur.close()
        conn.close()
        raise HTTPException(status_code=404, detail="Account not found")
    conn.commit()
    cur.close()
    conn.close()
    return {"detail": f"Account {account_id} deleted successfully"}
