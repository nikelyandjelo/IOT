from datetime import time, datetime
from time import sleep

from app.entities.agent_data import AgentData
from app.entities.processed_agent_data import ProcessedAgentData

from app.entities.agent_data import InputData


def process_agent_data(
    agent_data: InputData,
) -> ProcessedAgentData:
    """
    Process agent data and classify the state of the road surface.
    Parameters:
        agent_data (AgentData): Agent data that containing accelerometer, GPS, and timestamp.
    Returns:
        processed_data_batch (ProcessedAgentData): Processed data containing the classified state of the road surface and agent data.
    """
    z_value = agent_data.accelerometer.z
    road_state = "flat"

    if z_value >=15000 :
        road_state="bump"
    elif z_value < 15000:
        road_state = "hole"
    sleep(1)
    return ProcessedAgentData(road_state=road_state, user_id=1, agent_data=AgentData(accelerometer=agent_data.accelerometer,
                                                   gps=agent_data.gps,
                                                   timestamp=agent_data.time, user_id = 1))