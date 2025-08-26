from fastapi import FastAPI

app = FastAPI()

# Sample user data with three users
users = [
    {
        "id": 1,
        "first_name": "John",
        "last_name": "Doe",
        "email": "john.doe@example.com",
        "created_at": "2025-01-15T09:30:00Z"
    },
    {
        "id": 2,
        "first_name": "Jane",
        "last_name": "Smith",
        "email": "jane.smith@example.com",
        "created_at": "2025-01-16T10:00:00Z"
    },
    {
        "id": 3,
        "first_name": "Alice",
        "last_name": "Johnson",
        "email": "alice.johnson@example.com",
        "created_at": "2025-01-17T11:15:00Z"
    }
]

@app.get("/users")
async def get_users():
    return users