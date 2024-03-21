import asyncio
import json
from typing import Set, List
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Float, DateTime, select, update, delete
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import NoResultFound
from datetime import datetime
from pydantic import BaseModel, validator
from config import (
    POSTGRES_HOST,
    POSTGRES_PORT,
    POSTGRES_DB,
    POSTGRES_USER,
    POSTGRES_PASSWORD,
)

# FastAPI app setup
app = FastAPI()

# SQLAlchemy setup
DATABASE_URL = f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
engine = create_engine(DATABASE_URL)
metadata = MetaData()

# Define the ProcessedAgentData table
processed_agent_data = Table(
    "processed_agent_data",
    metadata,
    Column("id", Integer, primary_key=True, index=True),
    Column("road_state", String),
    Column("user_id", Integer),
    Column("x", Float),
    Column("y", Float),
    Column("z", Float),
    Column("latitude", Float),
    Column("longitude", Float),
    Column("timestamp", DateTime),
)
SessionLocal = sessionmaker(bind=engine)


# SQLAlchemy model
class ProcessedAgentDataInDB(BaseModel):
    id: int
    road_state: str
    user_id: int
    x: float
    y: float
    z: float
    latitude: float
    longitude: float
    timestamp: datetime


# FastAPI models
class AccelerometerData(BaseModel):
    x: float
    y: float
    z: float


class GpsData(BaseModel):
    latitude: float
    longitude: float


class AgentData(BaseModel):
    user_id: int
    accelerometer: AccelerometerData
    gps: GpsData
    timestamp: datetime

    @validator("timestamp", pre=True, always=True)
    def parse_timestamp(cls, v):
        if isinstance(v, datetime):
            return v
        try:
            return datetime.fromisoformat(v)
        except (TypeError, ValueError):
            raise ValueError(
                "Invalid timestamp format. Expected ISO 8601 format (YYYY-MM-DDTHH:MM:SSZ)."
            )


class ProcessedAgentData(BaseModel):
    road_state: str
    agent_data: AgentData


# WebSocket subscriptions
subscriptions: Set[WebSocket] = set()


# FastAPI WebSocket endpoint
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    subscriptions.add(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        subscriptions.remove(websocket)


# Function to send data to subscribed users
async def send_data_to_subscribers(data):
    for websocket in subscriptions:
        await websocket.send_json(data)


# FastAPI CRUD endpoints
@app.post("/processed_agent_data/")
async def create_processed_agent_data(data: List[ProcessedAgentData]):
    with SessionLocal() as db:
        results = []
        for item in data:
            try:
                record = {
                    "road_state": item.road_state,
                    "user_id": item.agent_data.user_id,
                    "x": item.agent_data.accelerometer.x,
                    "y": item.agent_data.accelerometer.y,
                    "z": item.agent_data.accelerometer.z,
                    "latitude": item.agent_data.gps.latitude,
                    "longitude": item.agent_data.gps.longitude,
                    "timestamp": item.agent_data.timestamp.isoformat()
                }
                ins = processed_agent_data.insert().values(record)
                db.execute(ins)
                db.commit()
                results.append(record)
            except Exception as e:
                db.rollback()
                raise e
        if results:
            await send_data_to_subscribers(results)


@app.get(
    "/processed_agent_data/{processed_agent_data_id}",
    response_model=ProcessedAgentDataInDB,
)
def read_processed_agent_data(processed_agent_data_id: int):
    with SessionLocal() as db:
        stmt = select(processed_agent_data).where(processed_agent_data.c.id == processed_agent_data_id)
        result = db.execute(stmt).fetchone()
        if not result:
            raise HTTPException(status_code=404, detail="ProcessedAgentData not found")
        return ProcessedAgentDataInDB(**result._asdict())


@app.get("/processed_agent_data/", response_model=List[ProcessedAgentDataInDB])
def list_processed_agent_data():
    with SessionLocal() as db:
        stmt = select(processed_agent_data)
        results = db.execute(stmt).fetchall()
        return [ProcessedAgentDataInDB(**row._asdict()) for row in results]


@app.put(
    "/processed_agent_data/{processed_agent_data_id}",
    response_model=ProcessedAgentDataInDB,
)
def update_processed_agent_data(processed_agent_data_id: int, data: ProcessedAgentData):
    with SessionLocal() as db:
        stmt = update(processed_agent_data).where(processed_agent_data.c.id == processed_agent_data_id).values(
            road_state=data.road_state,
            user_id=data.agent_data.user_id,
            x=data.agent_data.accelerometer.x,
            y=data.agent_data.accelerometer.y,
            z=data.agent_data.accelerometer.z,
            latitude=data.agent_data.gps.latitude,
            longitude=data.agent_data.gps.longitude,
            timestamp=data.agent_data.timestamp
        )
        result = db.execute(stmt)
        if result.rowcount == 0:
            raise HTTPException(status_code=404, detail="ProcessedAgentData not found")
        db.commit()
        try:
            updated_stmt = select(processed_agent_data).where(processed_agent_data.c.id == processed_agent_data_id)
            updated_result = db.execute(updated_stmt).one()
            return ProcessedAgentDataInDB(**updated_result._asdict())
        except NoResultFound:
            raise HTTPException(status_code=404, detail="Failed to fetch updated ProcessedAgentData")


@app.delete(
    "/processed_agent_data/{processed_agent_data_id}",
    response_model=ProcessedAgentDataInDB,
)
def delete_processed_agent_data(processed_agent_data_id: int):
    with SessionLocal() as db:
        stmt = select(processed_agent_data).where(processed_agent_data.c.id == processed_agent_data_id)
        result = db.execute(stmt).fetchone()
        if not result:
            raise HTTPException(status_code=404, detail="ProcessedAgentData not found")
        stmt = delete(processed_agent_data).where(processed_agent_data.c.id == processed_agent_data_id)
        db.execute(stmt)
        db.commit()
        return ProcessedAgentDataInDB(**result._asdict())


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=8000)
