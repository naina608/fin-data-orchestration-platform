from fastapi import FastAPI
from apis import customers, accounts, transactions

app = FastAPI(title="Banking Modern Stack API")

# Include routers
app.include_router(customers.router)
app.include_router(accounts.router)
app.include_router(transactions.router)
