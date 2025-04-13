"""
Script to generate test data for the Messenger application.
This script is a skeleton for students to implement.
"""
import os
import uuid
import logging
import random
from datetime import datetime, timedelta
from cassandra.cluster import Cluster

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Cassandra connection settings
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "localhost")
CASSANDRA_PORT = int(os.getenv("CASSANDRA_PORT", "9042"))
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "messenger")

# Test data configuration
NUM_USERS = 10  # Number of users to create
NUM_CONVERSATIONS = 15  # Number of conversations to create
MAX_MESSAGES_PER_CONVERSATION = 50  # Maximum number of messages per conversation

def connect_to_cassandra():
    """Connect to Cassandra cluster."""
    logger.info("Connecting to Cassandra...")
    try:
        cluster = Cluster([CASSANDRA_HOST])
        session = cluster.connect(CASSANDRA_KEYSPACE)
        logger.info("Connected to Cassandra!")
        return cluster, session
    except Exception as e:
        logger.error(f"Failed to connect to Cassandra: {str(e)}")
        raise

def generate_test_data(session):
    """
    Generate test data in Cassandra.
    
    Students should implement this function to generate test data based on their schema design.
    The function should create:
    - Users (with IDs 1-NUM_USERS)
    - Conversations between random pairs of users
    - Messages in each conversation with realistic timestamps
    """
    logger.info("Generating test data...")
    
    # TODO: Students should implement the test data generation logic
    # Hint:
    # 1. Create a set of user IDs
    # 2. Create conversations between random pairs of users
    # 3. For each conversation, generate a random number of messages
    # 4. Update relevant tables to maintain data consistency
    
    logger.info("Generating test data...")
    
    # Generate user data
    users = []
    for user_id in range(1, NUM_USERS + 1):
        user_uuid = uuid.uuid4()  # Generating random UUID for each user
        users.append(user_uuid)
        name = f"User {user_id}"
        session.execute(
            "INSERT INTO users (user_id, name) VALUES (%s, %s)",
            (user_uuid, name)
        )
        logger.info(f"Created user: {name} (ID: {user_uuid})")

    # Generate conversations between random users
    for _ in range(NUM_CONVERSATIONS):
        user1_id, user2_id = random.sample(users, 2)  # Random pair of users
        conversation_id = uuid.uuid4()  # Unique conversation ID
        start_time = datetime.now() - timedelta(days=random.randint(1, 30))  # Random start time within the last 30 days
        session.execute(
            "INSERT INTO conversations (conversation_id, user1_id, user2_id, start_time) VALUES (%s, %s, %s, %s)",
            (conversation_id, user1_id, user2_id, start_time)
        )
        logger.info(f"Created conversation between {user1_id} and {user2_id} (ID: {conversation_id})")

        # Generate random messages for this conversation
        num_messages = random.randint(1, MAX_MESSAGES_PER_CONVERSATION)  # Random number of messages
        for _ in range(num_messages):
            sender_id = random.choice([user1_id, user2_id])  # Random sender
            message_text = f"Message from {sender_id} at {datetime.now()}"
            message_timestamp = datetime.now() - timedelta(minutes=random.randint(1, 60))  # Random timestamp
            message_id = uuid.uuid4()  # Unique message ID
            session.execute(
                "INSERT INTO messages (message_id, conversation_id, sender_id, timestamp, message_text) VALUES (%s, %s, %s, %s, %s)",
                (message_id, conversation_id, sender_id, message_timestamp, message_text)
            )
            logger.info(f"Created message (ID: {message_id}) in conversation {conversation_id}")
    
    logger.info(f"Generated {NUM_CONVERSATIONS} conversations with messages")
    logger.info(f"User IDs range from 1 to {NUM_USERS}")
    logger.info("Use these IDs for testing the API endpoints")

def main():
    """Main function to generate test data."""
    cluster = None
    
    try:
        # Connect to Cassandra
        cluster, session = connect_to_cassandra()
        
        # Generate test data
        generate_test_data(session)
        
        logger.info("Test data generation completed successfully!")
    except Exception as e:
        logger.error(f"Error generating test data: {str(e)}")
    finally:
        if cluster:
            cluster.shutdown()
            logger.info("Cassandra connection closed")

if __name__ == "__main__":
    main() 